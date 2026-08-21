<?php

declare(strict_types=1);

namespace LesQueue\Config;

use LesQueue\DbalQueue;
use LesQueue\PgsqlQueue;
use LesQueue\RabbitMqQueue;
use LesQueue\Config\Factory\DbalQueueFactory;
use LesQueue\Config\Factory\PgsqlQueueFactory;
use LesQueue\Config\Factory\RabbitMqQueueFactory;

/**
 * @psalm-immutable
 */
final class ConfigProvider
{
    /**
     * @return array<string, mixed>
     *
     * @psalm-pure
     *
     * @psalm-suppress DeprecatedClass
     */
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                'factories' => [
                    // @phpstan-ignore-next-line
                    RabbitMqQueue::class => RabbitMqQueueFactory::class,
                    DbalQueue::class => DbalQueueFactory::class,
                    PgsqlQueue::class => PgsqlQueueFactory::class,
                ],
            ],
        ];
    }
}
