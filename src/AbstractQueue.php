<?php

declare(strict_types=1);

namespace LesQueue;

use Override;
use LesQueue\Job\Job;
use LesQueue\Job\Property\Name;
use LesQueue\Parameter\Priority;
use LesValueObject\Number\Int\Date\Timestamp;
use LesValueObject\Composite\DynamicCompositeValueObject;

abstract class AbstractQueue implements Queue
{
    #[Override]
    public function publish(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $this->insert(
            $name,
            $data,
            $until,
            $priority,
        );
    }

    #[Override]
    public function republish(Job $job, Timestamp $until, ?Priority $priority = null): void
    {
        $this->insert(
            $job->name,
            $job->data,
            $until,
            $priority,
            $job->attempt->value + 1,
        );
    }

    abstract protected function insert(
        Name $name,
        DynamicCompositeValueObject $data,
        ?Timestamp $until = null,
        ?Priority $priority = null,
        int $attempt = 0,
    ): void;
}
