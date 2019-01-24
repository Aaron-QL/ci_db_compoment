<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-22
 * Time: 14:39
 */

namespace OFashion\DAO\PDO;

use OFashion\DAO\Contracts\DB_Result;
use OFashion\DAO\Exceptions\DB_Adapt_Exception;
use PDO;
use stdClass;


class PDO_Result extends DB_Result
{

    /**
     * Number of rows in the result set
     *
     * @return    int
     */
    public function num_rows()
    {
        if (is_int($this->num_rows)) {
            return $this->num_rows;
        } elseif (count($this->result_array) > 0) {
            return $this->num_rows = count($this->result_array);
        } elseif (count($this->result_object) > 0) {
            return $this->num_rows = count($this->result_object);
        } elseif (($num_rows = $this->result_id->rowCount()) > 0) {
            return $this->num_rows = $num_rows;
        }

        return $this->num_rows = count($this->result_array());
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @return    int
     */
    public function num_fields()
    {
        return $this->result_id->columnCount();
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @return array|bool
     */
    public function list_fields()
    {
        $field_names = [];
        for ($i = 0, $c = $this->num_fields(); $i < $c; $i++) {
            // Might trigger an E_WARNING due to not all subdrivers
            // supporting getColumnMeta()
            $field_names[$i] = @$this->result_id->getColumnMeta($i);
            $field_names[$i] = $field_names[$i]['name'];
        }

        return $field_names;
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @return array|bool
     */
    public function field_data()
    {
        try {
            $retval = [];

            for ($i = 0, $c = $this->num_fields(); $i < $c; $i++) {
                $field = $this->result_id->getColumnMeta($i);

                $retval[$i] = new stdClass();
                $retval[$i]->name = $field['name'];
                $retval[$i]->type = $field['native_type'];
                $retval[$i]->max_length = ($field['len'] > 0) ? $field['len'] : null;
                $retval[$i]->primary_key = (int)(!empty($field['flags']) && in_array('primary_key', $field['flags'], true));
            }

            return $retval;
        } catch (\Exception $e) {
            throw new DB_Adapt_Exception($e->getMessage(), 0, $e);
        }
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @return    void
     */
    public function free_result()
    {
        if (is_object($this->result_id)) {
            $this->result_id = false;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @return    array
     */
    protected function _fetch_assoc()
    {
        return $this->result_id->fetch(PDO::FETCH_ASSOC);
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param    string $class_name
     * @return    object
     */
    protected function _fetch_object($class_name = 'stdClass')
    {
        return $this->result_id->fetchObject($class_name);
    }

}
