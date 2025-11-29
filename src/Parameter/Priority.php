<?php

declare(strict_types=1);

namespace LesQueue\Parameter;

use Override;
use LesValueObject\Number\Int\AbstractIntValueObject;

/**
 * @psalm-immutable
 */
final class Priority extends AbstractIntValueObject
{
    public static function low(): self
    {
        return new self(1);
    }

    public static function normal(): self
    {
        return new self(2);
    }

    public static function medium(): self
    {
        return new self(3);
    }

    public static function high(): self
    {
        return new self(4);
    }

    /**
     * @psalm-pure
     */    #[Override]
    public static function getMinimumValue(): int
    {
        return 0;
    }

    /**
     * @psalm-pure
     */    #[Override]
    public static function getMaximumValue(): int
    {
        return 5;
    }
}
