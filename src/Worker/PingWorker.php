<?php

declare(strict_types=1);

namespace LesQueue\Worker;

use Override;
use LesQueue\Job\Job;

/**
 * @deprecated will be dropped
 */
final class PingWorker implements Worker
{
    #[Override]
    public function process(Job $job): void
    {
        echo 'pong';
    }
}
