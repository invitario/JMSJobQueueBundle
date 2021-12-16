<?php

namespace JMS\JobQueueBundle\Retry;

use DateTime;
use Exception;
use JMS\JobQueueBundle\Entity\Job;

class ExponentialRetryScheduler implements RetryScheduler
{
    private $base;

    public function __construct($base = 5)
    {
        $this->base = $base;
    }

    /**
     * @throws Exception
     */
    public function scheduleNextRetry(Job $originalJob): DateTime
    {
        return new DateTime('+'.(pow($this->base, count($originalJob->getRetryJobs()))).' seconds');
    }
}