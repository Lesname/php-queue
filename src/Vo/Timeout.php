<?php
declare(strict_types=1);

namespace LessQueue\Vo;

use LessValueObject\Number\Int\AbstractIntValueObject;

/**
 * @psalm-immutable
 */
final class Timeout extends AbstractIntValueObject
{
    /**
     * @psalm-pure
     */
    public static function getMinValue(): int
    {
        return 1;
    }

    /**
     * @psalm-pure
     */
    public static function getMaxValue(): int
    {
        return 3_600;
    }
}
