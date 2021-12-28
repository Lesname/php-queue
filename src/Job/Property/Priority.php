<?php
declare(strict_types=1);

namespace LessQueue\Job\Property;

use LessValueObject\Number\Int\AbstractIntValueObject;

/**
 * @psalm-immutable
 */
final class Priority extends AbstractIntValueObject
{
    /**
     * @psalm-pure
     */
    public static function lowest(): self
    {
        return new self(-9);
    }

    /**
     * @psalm-pure
     */
    public static function lower(): self
    {
        return new self(-6);
    }

    /**
     * @psalm-pure
     */
    public static function low(): self
    {
        return new self(-3);
    }

    /**
     * @psalm-pure
     */
    public static function default(): self
    {
        return new self(0);
    }

    /**
     * @psalm-pure
     */
    public static function high(): self
    {
        return new self(3);
    }

    /**
     * @psalm-pure
     */
    public static function higher(): self
    {
        return new self(6);
    }

    /**
     * @psalm-pure
     */
    public static function highest(): self
    {
        return new self(9);
    }

    /**
     * @psalm-pure
     */
    public static function getMinValue(): int
    {
        return -9;
    }

    /**
     * @psalm-pure
     */
    public static function getMaxValue(): int
    {
        return 9;
    }
}
