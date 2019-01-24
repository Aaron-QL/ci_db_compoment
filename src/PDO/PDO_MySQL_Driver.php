<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-22
 * Time: 15:18
 */

namespace OFashion\DAO\PDO;

use PDO;
use stdClass;

class PDO_MySQL_Driver extends PDO_Driver
{

    /**
     * Sub-driver
     *
     * @var    string
     */
    public $subdriver = 'mysql';

    /**
     * Compression flag
     *
     * @var    bool
     */
    public $compress = false;

    /**
     * Strict ON flag
     *
     * Whether we're running in strict SQL mode.
     *
     * @var    bool
     */
    public $stricton;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '`';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
     *
     * @param    array $params
     * @return    void
     */
    public function __construct($params)
    {
        parent::__construct($params);

        if (!empty($this->dsn)) {
            $this->dsn = 'mysql:host=' . (empty($this->hostname) ? '127.0.0.1' : $this->hostname);

            empty($this->port) OR $this->dsn .= ';port=' . $this->port;
            empty($this->database) OR $this->dsn .= ';dbname=' . $this->database;
            empty($this->char_set) OR $this->dsn .= ';charset=' . $this->char_set;
        } elseif (!empty($this->char_set) && strpos($this->dsn, 'charset=', 6) === false) {
            $this->dsn .= ';charset=' . $this->char_set;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param    bool $persistent
     * @return    object
     */
    public function db_connect($persistent = false)
    {
        if (isset($this->stricton)) {
            if ($this->stricton) {
                $sql = 'CONCAT(@@sql_mode, ",", "STRICT_ALL_TABLES")';
            } else {
                $sql = 'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        @@sql_mode,
                                        "STRICT_ALL_TABLES,", ""),
                                        ",STRICT_ALL_TABLES", ""),
                                        "STRICT_ALL_TABLES", ""),
                                        "STRICT_TRANS_TABLES,", ""),
                                        ",STRICT_TRANS_TABLES", ""),
                                        "STRICT_TRANS_TABLES", "")';
            }

            if (!empty($sql)) {
                if (empty($this->options[PDO::MYSQL_ATTR_INIT_COMMAND])) {
                    $this->options[PDO::MYSQL_ATTR_INIT_COMMAND] = 'SET SESSION sql_mode = ' . $sql;
                } else {
                    $this->options[PDO::MYSQL_ATTR_INIT_COMMAND] .= ', @@session.sql_mode = ' . $sql;
                }
            }
        }

        if ($this->compress === true) {
            $this->options[PDO::MYSQL_ATTR_COMPRESS] = true;
        }

        if (is_array($this->encrypt)) {
            $ssl = [];
            empty($this->encrypt['ssl_key']) OR $ssl[PDO::MYSQL_ATTR_SSL_KEY] = $this->encrypt['ssl_key'];
            empty($this->encrypt['ssl_cert']) OR $ssl[PDO::MYSQL_ATTR_SSL_CERT] = $this->encrypt['ssl_cert'];
            empty($this->encrypt['ssl_ca']) OR $ssl[PDO::MYSQL_ATTR_SSL_CA] = $this->encrypt['ssl_ca'];
            empty($this->encrypt['ssl_capath']) OR $ssl[PDO::MYSQL_ATTR_SSL_CAPATH] = $this->encrypt['ssl_capath'];
            empty($this->encrypt['ssl_cipher']) OR $ssl[PDO::MYSQL_ATTR_SSL_CIPHER] = $this->encrypt['ssl_cipher'];

            if (defined('PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT') && isset($this->encrypt['ssl_verify'])) {
                $ssl[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = $this->encrypt['ssl_verify'];
            }

            // DO NOT use array_merge() here!
            // It re-indexes numeric keys and the PDO_MYSQL_ATTR_SSL_* constants are integers.
            empty($ssl) OR $this->options += $ssl;
        }

        // Prior to version 5.7.3, MySQL silently downgrades to an unencrypted connection if SSL setup fails
        if (
            ($pdo = parent::db_connect($persistent)) !== false
            && !empty($ssl)
            && version_compare($pdo->getAttribute(PDO::ATTR_CLIENT_VERSION), '5.7.3', '<=')
            && empty($pdo->query("SHOW STATUS LIKE 'ssl_cipher'")->fetchObject()->Value)
        ) {
            $message = 'PDO_MYSQL was configured for an SSL connection, but got an unencrypted connection instead!';
            return ($this->db_debug) ? $this->display_error($message, '', true) : false;
        }

        return $pdo;
    }

    // --------------------------------------------------------------------

    /**
     * Select the database
     *
     * @param    string $database
     * @return    bool
     */
    public function db_select($database = '')
    {
        if ($database === '') {
            $database = $this->database;
        }

        if (false !== $this->simple_query('USE ' . $this->escape_identifiers($database))) {
            $this->database = $database;
            $this->data_cache = [];
            return true;
        }

        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @return    bool
     */
    protected function _trans_begin()
    {
        $this->conn_id->setAttribute(PDO::ATTR_AUTOCOMMIT, false);
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
        if ($this->conn_id->commit()) {
            $this->conn_id->setAttribute(PDO::ATTR_AUTOCOMMIT, true);
            return true;
        }

        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @return    bool
     */
    protected function _trans_rollback()
    {
        if ($this->conn_id->rollBack()) {
            $this->conn_id->setAttribute(PDO::ATTR_AUTOCOMMIT, true);
            return true;
        }

        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Show column query
     *
     * Generates a platform-specific query string so that the column names can be fetched
     *
     * @param    string $table
     * @return    string
     */
    protected function _list_columns($table = '')
    {
        return 'SHOW COLUMNS FROM ' . $this->protect_identifiers($table, true, null, false);
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param    string $table
     * @return array|bool
     */
    public function field_data($table)
    {
        if (($query = $this->query('SHOW COLUMNS FROM ' . $this->protect_identifiers($table, true, null, false))) === false) {
            return false;
        }
        $query = $query->result_object();

        $retval = [];
        for ($i = 0, $c = count($query); $i < $c; $i++) {
            $retval[$i] = new stdClass();
            $retval[$i]->name = $query[$i]->Field;

            sscanf($query[$i]->Type, '%[a-z](%d)',
                $retval[$i]->type,
                $retval[$i]->max_length
            );

            $retval[$i]->default = $query[$i]->Default;
            $retval[$i]->primary_key = (int)($query[$i]->Key === 'PRI');
        }

        return $retval;
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
        return 'TRUNCATE ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * @return    string
     */
    protected function _from_tables()
    {
        if (!empty($this->qb_join) && count($this->qb_from) > 1) {
            return '(' . implode(', ', $this->qb_from) . ')';
        }

        return implode(', ', $this->qb_from);
    }

}
