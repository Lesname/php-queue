<?php
declare(strict_types=1);

namespace LessQueue\Job;

use LessValueObject\Number\Int\Unsigned;

/**
 * @psalm-immutable
 */
final class DbalJob implements Job
{
    /**
     * @param array<mixed> $data
     */
    public function __construct(
        public readonly string $id,
        private readonly Property\Name $name,
        private readonly Property\Priority $priority,
        private readonly array $data,
        private readonly Unsigned $attempt,
    ) {}

    public function getName(): Property\Name
    {
        return $this->name;
    }

    public function getPriority(): Property\Priority
    {
        return $this->priority;
    }

    /**
     * @return array<mixed>
     */
    public function getData(): array
    {
        return $this->data;
    }

    public function getAttempt(): Unsigned
    {
        return $this->attempt;
    }
}
