<?php

declare(strict_types=1);

namespace LesQueue\Worker;

use LesQueue\Job\Job;

/**
 * @psalm-mutable
 */
interface Worker
{
    /**
     * @psalm-impure
     */
    public function process(Job $job): void;
}
