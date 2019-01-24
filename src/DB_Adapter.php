<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-21
 * Time: 17:07
 */
declare(strict_types=1);
namespace OFashion\DAO;

use OFashion\DAO\Exceptions\DB_Adapt_Exception;
use Closure;
use OFashion\DAO\PDO\PDO_MySQL_Driver;

class DB_Adapter
{
    private static $connMap;

    private static $db;

    public static function loadConfiguration(Closure $callback)
    {
        static::$db = call_user_func($callback);
    }

    public static function getConnection($activeGroup = 'db')
    {
        if (isset(static::$connMap[$activeGroup])) {
            return static::$connMap[$activeGroup];
        }

        if (!isset(static::$db[$activeGroup])) {
            throw new DB_Adapt_Exception($activeGroup . 'not found');
        }

        try {
            $conn = new PDO_MySQL_Driver(static::$db[$activeGroup]);
            $conn->initialize();
            return static::$connMap[$activeGroup] = $conn;
        } catch (\Throwable $t) {
            throw new DB_Adapt_Exception($t->getMessage());
        }
    }
}