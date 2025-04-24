<?php
declare(strict_types=1);

namespace LesQueue\Job;

use LesValueObject\Composite\DynamicCompositeValueObject;
use LesValueObject\Composite\AbstractCompositeValueObject;
use LesValueObject\Number\Int\Unsigned;

/**
 * @psalm-immutable
 */
final class Job extends AbstractCompositeValueObject
{
    public function __construct(
        public readonly Property\Identifier $id,
        public readonly Property\Name $name,
        public readonly DynamicCompositeValueObject $data,
        public readonly Unsigned $attempt,
    ) {}
}
