<?php
declare(strict_types=1);

namespace LessQueue\Worker;

use LessQueue\Job\Job;

final class PingWorker implements Worker
{
    public function process(Job $job): void
    {
        echo 'pong';
    }
}
