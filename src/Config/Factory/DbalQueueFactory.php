<?php

declare(strict_types=1);

namespace LesQueue\Config\Factory;

use LesQueue\DbalQueue;
use Doctrine\DBAL\Connection;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use Psr\Container\ContainerExceptionInterface;

final class DbalQueueFactory
{
    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function __invoke(ContainerInterface $container): DbalQueue
    {
        $connection = $container->get(Connection::class);
        assert($connection instanceof Connection);

        return new DbalQueue($connection);
    }
}
