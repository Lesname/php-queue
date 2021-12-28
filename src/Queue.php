<?php
declare(strict_types=1);

namespace LessQueue;

use LessQueue\Job\Job;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use LessValueObject\Number\Int\Date\Timestamp;

interface Queue
{
    /**
     * @param Name $name
     * @param array<string, mixed> $data
     * @param Timestamp|null $until
     * @param Priority|null $priority
     */
    public function put(Name $name, array $data, ?Timestamp $until = null, ?Priority $priority = null): Job;

    public function reserve(): ?Job;

    public function delete(Job | string $job): void;

    public function bury(Job $job): void;
}
