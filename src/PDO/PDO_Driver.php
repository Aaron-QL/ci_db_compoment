<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-22
 * Time: 14:37
 */

namespace OFashion\DAO\PDO;

use OFashion\DAO\Contracts\DB_Query_Builder;
use OFashion\DAO\Exceptions\DB_Adapt_Exception;
use PDO;
use PDOException;

class PDO_Driver extends DB_Query_Builder
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'pdo';

    /**
     * PDO Options
     *
     * @var    array
     */
    public $options = [];

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param    bool $persistent
     * @return bool|PDO
     */
    public function db_connect($persistent = false)
    {
        if ($persistent === true) {
            $this->options[PDO::ATTR_PERSISTENT] = true;
        }

        try {
            return new PDO($this->dsn, $this->username, $this->password, $this->options);
        } catch (PDOException $e) {
            throw new DB_Adapt_Exception($e->getMessage(), 0, $e);
        }
    }

    // --------------------------------------------`------------------------

    /**
     * Execute the query
     *
     * @param    string $sql SQL query
     * @return    mixed
     */
    protected function _execute($sql)
    {
        return $this->conn_id->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @return    bool
     */
    protected function _trans_begin()
    {
        return $this->conn_id->beginTransaction();
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @return    bool
     */
    protected function _trans_commit()
    {
        return $this->conn_id->commit();
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @return    bool
     */
    protected function _trans_rollback()
    {
        return $this->conn_id->rollBack();
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependent string escape
     *
     * @param    string
     * @return    string
     */
    protected function _escape_str($str)
    {
        // Escape the string
        $str = $this->conn_id->quote($str);

        // If there are duplicated quotes, trim them away
        return ($str[0] === "'")
            ? substr($str, 1, -1)
            : $str;
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @return    int
     */
    public function affected_rows()
    {
        return is_object($this->result_id) ? $this->result_id->rowCount() : 0;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @param    string $name
     * @return    int
     */
    public function insert_id($name = null)
    {
        return $this->conn_id->lastInsertId($name);
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @param    string $table
     * @return    string
     */
    protected function _field_data($table)
    {
        return 'SELECT TOP 1 * FROM ' . $this->protect_identifiers($table);
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occurred.
     *
     * @return    array
     */
    public function error()
    {
        $error = ['code' => '00000', 'message' => ''];
        $pdo_error = $this->conn_id->errorInfo();

        if (empty($pdo_error[0])) {
            return $error;
        }

        $error['code'] = isset($pdo_error[1]) ? $pdo_error[0] . '/' . $pdo_error[1] : $pdo_error[0];
        if (isset($pdo_error[2])) {
            $error['message'] = $pdo_error[2];
        }

        return $error;
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the TRUNCATE statement,
     * then this method maps to 'DELETE FROM table'
     *
     * @param    string $table
     * @return    string
     */
    protected function _truncate($table)
    {
        return 'TRUNCATE TABLE ' . $table;
    }

}
