<?php

declare(strict_types=1);

namespace LesQueue\Worker;

use LesQueue\Job\Job;

interface Worker
{
    public function process(Job $job): void;
}
