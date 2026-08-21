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
    /**
     * @psalm-pure
     */
    public static function none(): self
    {
        return new self(-32);
    }

    /**
     * @psalm-pure
     */
    public static function low(): self
    {
        return new self(-8);
    }

    /**
     * @psalm-pure
     */
    public static function normal(): self
    {
        return new self(0);
    }

    /**
     * @psalm-pure
     */
    public static function medium(): self
    {
        return new self(8);
    }

    /**
     * @psalm-pure
     */
    public static function highest(): self
    {
        return new self(32);
    }

    /**
     * @psalm-pure
     */
    #[Override]
    public static function getMinimumValue(): int
    {
        return -32;
    }

    /**
     * @psalm-pure
     */
    #[Override]
    public static function getMaximumValue(): int
    {
        return 32;
    }
}
