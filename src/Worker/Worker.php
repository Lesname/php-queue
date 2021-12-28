<?php
declare(strict_types=1);

namespace LessQueue\Worker;

use LessQueue\Job\Job;

interface Worker
{
    public function process(Job $job): void;
}
