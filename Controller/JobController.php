<?php

namespace JMS\JobQueueBundle\Controller;

use App\Entity\Account;
use Doctrine\Common\Util\ClassUtils;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Query\ResultSetMappingBuilder;
use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Entity\Repository\JobManager;
use JMS\JobQueueBundle\View\JobFilter;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\RedirectResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Routing\Annotation\Route;

class JobController extends AbstractController
{

    /** @var EntityManagerInterface */
    private $em;

    /** @var JobManager */
    private $jobManager;

    /**
     * DefaultController constructor.
     * @param EntityManagerInterface $em
     * @param JobManager $jobManager
     */
    public function __construct(EntityManagerInterface $em, JobManager $jobManager)
    {
        $this->em = $em;
        $this->jobManager = $jobManager;
    }

    /**
     * @Route("/", name = "jms_jobs_overview")
     */
    public function overviewAction(Request $request): Response
    {
        $jobFilter = JobFilter::fromRequest($request);

        $perPage = 50;
        $lastJobsWithError = $jobFilter->isDefaultPage() ? $this->getRepo()->findLastJobsWithError(5) : [];
        $queryParams = [];

        $jobsWithErrorIdArray =array_map(
            function ($i) {
                return $i->getId();
            },
            $lastJobsWithError
        );
        $excludeErrorJobQuery = '';
        if(count($jobsWithErrorIdArray) > 0) {
            $excludeErrorJobQuery = 'AND j.id NOT IN(:excludeJobIds)';
            $queryParams['excludeJobIds'] = $jobsWithErrorIdArray;
        }

        $searchQuery = '';
        if ( ! empty($jobFilter->command)) {
            $searchQuery = "AND (j.command LIKE(:commandSearchString) OR args::text LIKE(:commandSearchString))";
            $queryParams['commandSearchString'] = '%' . $jobFilter->command . '%';
        }

        $stateQuery = '';
        if ( ! empty($jobFilter->state)) {
            $stateQuery = "AND j.state = :jobState";
            $queryParams['jobState'] = $jobFilter->state;
        }

        $limit = $perPage + 1;
        $offset = ($jobFilter->page - 1) * $perPage;

        $rsm = new ResultSetMappingBuilder($this->getEm());
        $rsm->addRootEntityFromClassMetadata(Job::class, 'job');
        $query = $this->getEm()->createNativeQuery(/** @lang=SQL */ "
            SELECT *
            FROM jms_jobs j, json_array_elements_text(case when j.args::text = '[]' then '[null]'::json else j.args end) args
            WHERE j.originaljob_id IS NULL
            $excludeErrorJobQuery
            $searchQuery
            $stateQuery
            ORDER BY j.id DESC
            LIMIT $limit
            OFFSET $offset
        ", $rsm);

        $query->setParameters($queryParams);
        $jobs = $query->getResult();

        return $this->render('@JMSJobQueue/Job/overview.html.twig', array(
            'jobsWithError' => $lastJobsWithError,
            'jobs' => array_slice($jobs, 0, $perPage),
            'jobFilter' => $jobFilter,
            'hasMore' => count($jobs) > $perPage,
            'jobStates' => Job::getStates(),
        ));
    }

    /**
     * @Route("/{id}", name = "jms_jobs_details")
     */
    public function detailsAction(Job $job): Response
    {
        $relatedEntities = array();
        foreach ($job->getRelatedEntities() as $entity) {
            $class = ClassUtils::getClass($entity);
            $relatedEntities[] = array(
                'class' => $class,
                'id' => json_encode($this->get('doctrine')->getManagerForClass($class)->getClassMetadata($class)->getIdentifierValues($entity)),
                'raw' => $entity,
            );
        }

        $statisticData = $statisticOptions = array();
        if ($this->getParameter('jms_job_queue.statistics')) {
            $dataPerCharacteristic = array();
            foreach ($this->get('doctrine')->getManagerForClass(Job::class)->getConnection()->query("SELECT * FROM jms_job_statistics WHERE job_id = ".$job->getId()) as $row) {
                $dataPerCharacteristic[$row['characteristic']][] = array(
                    // hack because postgresql lower-cases all column names.
                    array_key_exists('createdAt', $row) ? $row['createdAt'] : $row['createdat'],
                    array_key_exists('charValue', $row) ? $row['charValue'] : $row['charvalue'],
                );
            }

            if ($dataPerCharacteristic) {
                $statisticData = array(array_merge(array('Time'), $chars = array_keys($dataPerCharacteristic)));
                $startTime = strtotime($dataPerCharacteristic[$chars[0]][0][0]);
                $endTime = strtotime($dataPerCharacteristic[$chars[0]][count($dataPerCharacteristic[$chars[0]])-1][0]);
                $scaleFactor = $endTime - $startTime > 300 ? 1/60 : 1;

                // This assumes that we have the same number of rows for each characteristic.
                for ($i=0,$c=count(reset($dataPerCharacteristic)); $i<$c; $i++) {
                    $row = array((strtotime($dataPerCharacteristic[$chars[0]][$i][0]) - $startTime) * $scaleFactor);
                    foreach ($chars as $name) {
                        $value = (float) $dataPerCharacteristic[$name][$i][1];

                        switch ($name) {
                            case 'memory':
                                $value /= 1024 * 1024;
                                break;
                        }

                        $row[] = $value;
                    }

                    $statisticData[] = $row;
                }
            }
        }

        return $this->render('@JMSJobQueue/Job/details.html.twig', array(
            'job' => $job,
            'relatedEntities' => $relatedEntities,
            'incomingDependencies' => $this->getRepo()->getIncomingDependencies($job),
            'statisticData' => $statisticData,
            'statisticOptions' => $statisticOptions,
        ));
    }

    /**
     * @Route("/{id}/retry", name = "jms_jobs_retry_job")
     */
    public function retryJobAction(Job $job): RedirectResponse
    {
        $state = $job->getState();

        if (
            Job::STATE_FAILED !== $state &&
            Job::STATE_TERMINATED !== $state &&
            Job::STATE_INCOMPLETE !== $state
        ) {
            throw new HttpException(400, 'Given job can\'t be retried');
        }

        $retryJob = clone $job;

        $this->getEm()->persist($retryJob);
        $this->getEm()->flush();

        $url = $this->generateUrl('jms_jobs_details', array('id' => $retryJob->getId()));

        return new RedirectResponse($url, 201);
    }

    private function getEm(): EntityManagerInterface
    {
        return $this->em;
    }

    private function getRepo(): JobManager
    {
        return $this->jobManager;
    }
}
