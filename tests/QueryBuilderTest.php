<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-23
 * Time: 19:02
 */
use PHPUnit\Framework\TestCase;
use OFashion\DAO\DB_Adapter;

final class QueryBuilderTest extends TestCase
{
    private $db;

    public function __construct(?string $name = null, array $data = [], string $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $localDir = __DIR__;
        DB_Adapter::loadConfiguration(function () use ($localDir) {
            return require $localDir . '/database.php';
        });
        $this->db = DB_Adapter::getConnection('local');
        $this->reset();
    }

    public function reset()
    {
        $this->db->query('truncate table_a');
        $this->db->query('truncate table_b');
        for ($i = 1; $i < 10; $i++) {
            $this->db->insert('table_a', ['id' => $i, 'a' => $i, 'b' => $i*2]);
            $this->db->insert('table_b', ['id' => $i, 'a' => $i, 'b' => $i*2]);
        }
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_count_all()
    {
        $this->assertEquals(9, $this->db->count_all('table_a'));
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_count_all_results()
    {
        $this->assertEquals(1, $this->db->like('a', '1')->count_all_results('table_a'));
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_count_all_results_limit()
    {
        $this->assertEquals(1, $this->db->like('a', '1')->limit(1)->count_all_results('table_a'));
    }


    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_delete()
    {
        // Check initial record
        $job1 = $this->db->where('id', 1)->get('table_a')->row();

        $this->assertEquals('1', $job1->a);

        // Do the delete
        $this->db->delete('table_a', array('id' => 1));

        // Check the record
        $job1 = $this->db->where('id', 1)->get('table_a');

        $this->assertEmpty($job1->result_array());

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_delete_several_tables()
    {
        // Check initial record
        $user4 = $this->db->where('id', 2)->get('table_a')->row();
        $job4 = $this->db->where('id', 2)->get('table_b')->row();

        $this->assertEquals('2', $job4->a);
        $this->assertEquals('2', $user4->a);

        // Do the delete
        $this->db->delete(array('table_a', 'table_b'), array('id' => 2));

        // Check the record
        $job4 = $this->db->where('id', 2)->get('table_a');
        $user4 = $this->db->where('id', 2)->get('table_b');

        $this->assertEmpty($job4->result_array());
        $this->assertEmpty($user4->result_array());

        $this->reset();
    }


    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_distinct()
    {
        $users = $this->db->select('a')
            ->distinct()
            ->get('table_a')
            ->result_array();

        $this->assertCount(9, $users);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_empty_table()
    {
        // Check initial record
        $jobs = $this->db->get('table_a')->result_array();

        $this->assertCount(9, $jobs);

        // Do the empty
        $this->db->empty_table('table_a');

        // Check the record
        $jobs = $this->db->get('table_a');

        $this->assertEmpty($jobs->result_array());

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_escape_like_percent_sign()
    {
        // Escape the like string
        $string = $this->db->escape_like_str('\%1');

        $sql = "SELECT `a` FROM `table_a` WHERE `a` LIKE '$string%' ESCAPE '!';";

        $res = $this->db->query($sql)->result_array();

        // Check the result
        $this->assertCount(0, $res);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_escape_like_backslash_sign()
    {
        // Escape the like string
        $string = $this->db->escape_like_str('\\');

        $sql = "SELECT `a` FROM `table_a` WHERE `a` LIKE '$string%' ESCAPE '!';";

        $res = $this->db->query($sql)->result_array();

        // Check the result
        $this->assertCount(0, $res);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_from_simple()
    {
        $jobs = $this->db->from('table_a')
            ->get()
            ->result_array();

        $this->assertCount(9, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_from_with_where()
    {
        $job1 = $this->db->from('table_a')
            ->where('id', 1)
            ->get()
            ->row();

        $this->assertEquals('1', $job1->id);
        $this->assertEquals('1', $job1->a);
        $this->assertEquals('2', $job1->b);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_get_simple()
    {
        $jobs = $this->db->get('table_a')->result_array();

        // Dummy jobs contain 4 rows
        $this->assertCount(9, $jobs);

        // Check rows item
        $this->assertEquals('1', $jobs[0]['a']);
        $this->assertEquals('2', $jobs[1]['a']);
        $this->assertEquals('3', $jobs[2]['a']);
        $this->assertEquals('4', $jobs[3]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_get_where()
    {
        $job1 = $this->db->get_where('table_a', array('id' => 1))->result_array();

        // Dummy jobs contain 1 rows
        $this->assertCount(1, $job1);

        // Check rows item
        $this->assertEquals('1', $job1[0]['a']);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_group_by()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->get()
            ->result_array();

        $this->assertCount(9, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_having_by()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->having('SUM(id) > 2')
            ->get()
            ->result_array();

        $this->assertCount(7, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_having_in()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->having_in('SUM(id)', array(1, 2, 5))
            ->get()
            ->result_array();

        $this->assertCount(3, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_or_having_in()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->or_having_in('SUM(id)', array(1, 5))
            ->or_having_in('SUM(id)', array(2, 6))
            ->get()
            ->result_array();

        $this->assertCount(4, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_having_not_in()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->having_not_in('SUM(id)', array(3, 6))
            ->get()
            ->result_array();

        $this->assertCount(7, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_or_having_not_in()
    {
        $jobs = $this->db->select('a')
            ->from('table_a')
            ->group_by('a')
            ->or_having_not_in('SUM(id)', array(1, 2, 3))
            ->or_having_not_in('SUM(id)', array(1, 3, 4))
            ->get()
            ->result_array();

        $this->assertCount(7, $jobs);
    }
    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_insert()
    {
        $this->db->query('truncate table_a');

        $job_data = array('id' => 10, 'a' => 'Grocery Sales', 'b' => '1!');

        // Do normal insert
        $this->assertTrue($this->db->insert('table_a', $job_data));

        $job1 = $this->db->get('table_a')->row();

        // Check the result
        $this->assertEquals('Grocery Sales', $job1->a);

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_insert_batch()
    {
        $job_datas = array(
            array('id' => 10, 'a' => 'Commedian', 'description' => 'Theres something in your teeth'),
            array('id' => 11, 'a' => 'Cab Driver', 'description' => 'Iam yellow'),
        );


        $this->assertEquals(2, $this->db->insert_batch('table_a', $job_datas));

        $job_2 = $this->db->where('id', 10)->get('table_a')->row();
        $job_3 = $this->db->where('id', 11)->get('table_a')->row();

        // Check the result
        $this->assertEquals('Commedian', $job_2->a);
        $this->assertEquals('Cab Driver', $job_3->a);

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_join_simple()
    {
        $job_user = $this->db->select('table_a.id as a_id, table_a.a as a_a, table_b.id as b_id, table_b.a as b_a')
            ->from('table_a')
            ->join('table_b', 'table_b.id = table_a.id')
            ->get()
            ->result_array();

        // Check the result
        $this->assertEquals('1', $job_user[0]['a_a']);
        $this->assertEquals('1', $job_user[0]['b_a']);
    }

    // ------------------------------------------------------------------------

    public function test_join_escape_is_null()
    {
        $expected = 'SELECT '.$this->db->escape_identifiers('field')
            ."\nFROM ".$this->db->escape_identifiers('table_a')
            ."\nJOIN ".$this->db->escape_identifiers('table_b').' ON '.$this->db->escape_identifiers('field').' IS NULL';

        $this->assertEquals(
            $expected,
            $this->db->select('field')->from('table_a')->join('table_b', 'field IS NULL')->get_compiled_select()
        );

        $expected = 'SELECT '.$this->db->escape_identifiers('field')
            ."\nFROM ".$this->db->escape_identifiers('table_a')
            ."\nJOIN ".$this->db->escape_identifiers('table_b').' ON '.$this->db->escape_identifiers('field').' IS NOT NULL';

        $this->assertEquals(
            $expected,
            $this->db->select('field')->from('table_a')->join('table_b', 'field IS NOT NULL')->get_compiled_select()
        );
    }

    // ------------------------------------------------------------------------

    public function test_join_escape_multiple_conditions()
    {
        // We just need a valid query produced, not one that makes sense
        $fields = array($this->db->protect_identifiers('table_a.a'), $this->db->protect_identifiers('table_b.b'));

        $expected = 'SELECT '.implode(', ', $fields)
            ."\nFROM ".$this->db->escape_identifiers('table_a')
            ."\nLEFT JOIN ".$this->db->escape_identifiers('table_b').' ON '.implode(' = ', $fields)
            .' AND '.$fields[0]." = 'foo' AND ".$fields[1].' = 0';

        $result = $this->db->select('table_a.a, table_b.b')
            ->from('table_a')
            ->join('table_b', "table_a.a = table_b.b AND table_a.a = 'foo' AND table_b.b = 0", 'LEFT')
            ->get_compiled_select();

        $this->assertEquals($expected, $result);
    }

    // ------------------------------------------------------------------------

    public function test_join_escape_multiple_conditions_with_parentheses()
    {
        // We just need a valid query produced, not one that makes sense
        $fields = array($this->db->protect_identifiers('table_a.a'), $this->db->protect_identifiers('table_b.b'));

        $expected = 'SELECT '.implode(', ', $fields)
            ."\nFROM ".$this->db->escape_identifiers('table_a')
            ."\nRIGHT JOIN ".$this->db->escape_identifiers('table_b').' ON '.implode(' = ', $fields)
            .' AND ('.$fields[0]." = 'foo' OR ".$fields[1].' IS NULL)';

        $result = $this->db->select('table_a.a, table_b.b')
            ->from('table_a')
            ->join('table_b', "table_a.a = table_b.b AND (table_a.a = 'foo' OR table_b.b IS NULL)", 'RIGHT')
            ->get_compiled_select();

        $this->assertEquals($expected, $result);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_like()
    {
        $job1 = $this->db->like('a', '2')
            ->get('table_a')
            ->row();

        // Check the result
        $this->assertEquals('2', $job1->id);
        $this->assertEquals('2', $job1->a);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_or_like()
    {
        $jobs = $this->db->like('a', '1')
            ->or_like('a', '3')
            ->get('table_a')
            ->result_array();

        // Check the result
        $this->assertCount(2, $jobs);
        $this->assertEquals('1', $jobs[0]['a']);
        $this->assertEquals('3', $jobs[1]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_not_like()
    {
        $jobs = $this->db->not_like('a', '1')
            ->get('table_a')
            ->result_array();

        // Check the result
        $this->assertCount(8, $jobs);
        $this->assertEquals('2', $jobs[0]['a']);
        $this->assertEquals('3', $jobs[1]['a']);
        $this->assertEquals('4', $jobs[2]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_or_not_like()
    {
        $jobs = $this->db->like('a', 'an')
            ->or_not_like('a', 'veloper')
            ->get('table_a')
            ->result_array();

        // Check the result
        $this->assertCount(9, $jobs);
        $this->assertEquals('1', $jobs[0]['a']);
        $this->assertEquals('2', $jobs[1]['a']);
        $this->assertEquals('3', $jobs[2]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * GitHub issue #273
     *
     * @see ./mocks/schema/skeleton.php
     */
    public function test_like_spaces_and_tabs()
    {
        $spaces = $this->db->like('a', '   ')->get('table_a')->result_array();
        $tabs = $this->db->like('a', "\t")->get('table_a')->result_array();

        $this->assertCount(0, $spaces);
        $this->assertCount(0, $tabs);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_limit()
    {
        $jobs = $this->db->limit(2)
            ->get('table_a')
            ->result_array();

        $this->assertCount(2, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_limit_and_offset()
    {
        $jobs = $this->db->limit(2, 2)
            ->get('table_a')
            ->result_array();

        $this->assertCount(2, $jobs);
        $this->assertEquals('3', $jobs[0]['a']);
        $this->assertEquals('4', $jobs[1]['a']);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_order_ascending()
    {
        $jobs = $this->db->order_by('a', 'asc')
            ->get('table_a')
            ->result_array();

        // Check the result
        $this->assertCount(9, $jobs);
        $this->assertEquals('1', $jobs[0]['a']);
        $this->assertEquals('2', $jobs[1]['a']);
        $this->assertEquals('3', $jobs[2]['a']);
        $this->assertEquals('4', $jobs[3]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_order_descending()
    {
        $jobs = $this->db->order_by('a', 'desc')
            ->get('table_a')
            ->result_array();

        $this->assertCount(9, $jobs);
        $this->assertEquals('9', $jobs[0]['a']);
        $this->assertEquals('8', $jobs[1]['a']);
        $this->assertEquals('7', $jobs[2]['a']);
        $this->assertEquals('6', $jobs[3]['a']);
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_select_only_one_collumn()
    {
        $jobs_name = $this->db->select('a')
            ->get('table_a')
            ->result_array();

        // Check rows item
        $this->assertArrayHasKey('a',$jobs_name[0]);
        $this->assertArrayNotHasKey('id', $jobs_name[0]);
        $this->assertArrayNotHasKey('description', $jobs_name[0]);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_select_min()
    {
        $job_min = $this->db->select_min('id')
            ->get('table_a')
            ->row();

        // Minimum id was 1
        $this->assertEquals('1', $job_min->id);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_select_max()
    {
        $job_max = $this->db->select_max('id')
            ->get('table_a')
            ->row();

        // Maximum id was 4
        $this->assertEquals('9', $job_max->id);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_select_avg()
    {
        $job_avg = $this->db->select_avg('id')
            ->get('table_a')
            ->row();

        // Average should be 2.5
        $this->assertEquals('5.0000', $job_avg->id);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_select_sum()
    {
        $job_sum = $this->db->select_sum('id')
            ->get('table_a')
            ->row();

        // Sum of ids should be 10
        $this->assertEquals('45', $job_sum->id);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_truncate()
    {
        // Check initial record
        $jobs = $this->db->get('table_a')->result_array();
        $this->assertCount(9, $jobs);

        // Do the empty
        $this->db->truncate('table_a');

        // Check the record
        $jobs = $this->db->get('table_a');
        $this->assertEmpty($jobs->result_array());

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_truncate_with_from()
    {
        // Check initial record
        $users = $this->db->get('table_a')->result_array();
        $this->assertCount(9, $users);

        // Do the empty
        $this->db->from('table_a')->truncate();

        // Check the record
        $users = $this->db->get('table_a');
        $this->assertEmpty($users->result_array());

        $this->reset();
    }

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_update()
    {
        // Check initial record
        $job1 = $this->db->where('id', 1)->get('table_a')->row();
        $this->assertEquals('1', $job1->a);

        // Do the update
        $this->db->where('id', 1)->update('table_a', array('a' => 'Programmer'));

        // Check updated record
        $job1 = $this->db->where('id', 1)->get('table_a')->row();
        $this->assertEquals('Programmer', $job1->a);

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_update_with_set()
    {
        // Check initial record
        $job1 = $this->db->where('id', 4)->get('table_a')->row();
        $this->assertEquals('4', $job1->a);

        // Do the update
        $this->db->set('a', 'Vocalist');
        $this->db->update('table_a', NULL, 'id = 4');

        // Check updated record
        $job1 = $this->db->where('id', 4)->get('table_a')->row();
        $this->assertEquals('Vocalist', $job1->a);

        $this->reset();
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_simple_key_value()
    {
        $job1 = $this->db->where('id', 1)->get('table_a')->row();

        $this->assertEquals('1', $job1->id);
        $this->assertEquals('1', $job1->a);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_custom_key_value()
    {
        $jobs = $this->db->where('id !=', 1)->get('table_a')->result_array();
        $this->assertCount(8, $jobs);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_associative_array()
    {
        $where = array('id >' => 2, 'a !=' => '3');
        $jobs = $this->db->where($where)->get('table_a')->result_array();

        $this->assertCount(6, $jobs);

        // Should be Musician
        $job = current($jobs);
        $this->assertEquals('4', $job['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_custom_string()
    {
        $where = "id > 2 AND a != '4'";
        $jobs = $this->db->where($where)->get('table_a')->result_array();

        $this->assertCount(6, $jobs);

        // Should be Musician
        $job = current($jobs);
        $this->assertEquals('3', $job['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_or()
    {
        $jobs = $this->db->where('a !=', '6')
            ->or_where('id >', 3)
            ->get('table_a')
            ->result_array();

        $this->assertCount(9, $jobs);
        $this->assertEquals('1', $jobs[0]['a']);
        $this->assertEquals('2', $jobs[1]['a']);
        $this->assertEquals('3', $jobs[2]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_in()
    {
        $jobs = $this->db->where_in('a', array('2', '7'))
            ->get('table_a')
            ->result_array();

        $this->assertCount(2, $jobs);
        $this->assertEquals('2', $jobs[0]['a']);
        $this->assertEquals('7', $jobs[1]['a']);
    }

    // ------------------------------------------------------------------------

    /**
     * @see ./mocks/schema/skeleton.php
     */
    public function test_where_not_in()
    {
        $jobs = $this->db->where_not_in('a', array('1', '4'))
            ->get('table_a')
            ->result_array();

        $this->assertCount(7, $jobs);
        $this->assertEquals('2', $jobs[0]['a']);
        $this->assertEquals('3', $jobs[1]['a']);
    }

    // ------------------------------------------------------------------------

    public function test_issue4093()
    {
        $input = 'bar and baz or qux';
        $sql = $this->db->where('a', $input)->get_compiled_select('table_a');
        $this->assertEquals("'".$input."'", substr($sql, -20));
    }
}