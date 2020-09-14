<?php

namespace App\Libraries;

/**
 * CodeIgniter MongoDB Active Record Library
 * CodeIgniter 4.0+ PHP 7.0+ MongoDB 3.0+
 * Author 刘飞跃
 * Version 1.0
 * Class MongoDB
 * @package App\Libraries
 */
class MongoDB
{
    private $config = [
        'active' => 'default',
        'default' => [
            'no_auth' => TRUE,
            'hostname' => 'localhost',
            'port' => '27017',
            'username' => '',
            'password' => '',
            'database' => '',
            'db_debug' => TRUE,
            'return_as' => 'array',
            'read_preference' => 'primary',
            'read_concern' => 'local',
            'legacy_support' => TRUE,
        ],
        'test' => [
            'no_auth' => TRUE,
            'hostname' => 'localhost',
            'port' => '27017',
            'username' => '',
            'password' => '',
            'database' => '',
            'db_debug' => TRUE,
            'return_as' => 'array',
            'read_preference' => 'primary',
            'read_concern' => 'local',
            'legacy_support' => TRUE,
        ]
    ];
    private $param = [];
    private $activate;
    private $connect;
    private $db;
    private $hostname;
    private $port;
    private $database;
    private $username;
    private $password;
    private $debug;
    private $legacy_support;
    private $read_concern;
    private $read_preference;
    private $selects = [];
    private $updates = [];
    private $wheres = [];
    private $limit = 999999;
    private $offset = 0;
    private $sorts = [];
    private $return_as = 'array';

    /**
     * 自动检查Mongo-PECL扩展是否已安装/启用
     * 准备连接变量并建立到MongoDB的连接
     *
     * MongoDB constructor.
     * @param array $param
     * @throws \Exception
     */
    function __construct($param = [])
    {
        if (!class_exists('\MongoDB\Driver\Manager')) {
            throw new \Exception("The MongoDB PECL extension has not been installed or enabled", 500);
        }
        if (!empty($param)) {
            $this->param = $param;
        }
        $this->connect();
    }

    /**
     * 准备工作，处理各个参数之间关系
     * @throws \Exception
     */
    private function prepare()
    {
        if (is_array($this->param) && count($this->param) > 0 && isset($this->param['activate']) == TRUE) {
            $this->activate = $this->param['activate'];
        } else if (isset($this->config['active']) && !empty($this->config['active'])) {
            $this->activate = $this->config['active'];
        } else {
            throw new \Exception("MongoDB configuration is missing.", 500);
        }
        if (isset($this->config[$this->activate]) == TRUE) {
            if (empty($this->config[$this->activate]['hostname'])) {
                throw new \Exception("Hostname missing from mongodb config group : {$this->activate}", 500);
            } else {
                $this->hostname = trim($this->config[$this->activate]['hostname']);
            }
            if (empty($this->config[$this->activate]['port'])) {
                throw new \Exception("Port number missing from mongodb config group : {$this->activate}", 500);
            } else {
                $this->port = trim($this->config[$this->activate]['port']);
            }
            if (isset($this->config[$this->activate]['no_auth']) == FALSE
                && empty($this->config[$this->activate]['username'])) {
                throw new \Exception("Username missing from mongodb config group : {$this->activate}", 500);
            } else {
                $this->username = trim($this->config[$this->activate]['username']);
            }
            if (isset($this->config[$this->activate]['no_auth']) == FALSE
                && empty($this->config[$this->activate]['password'])) {
                throw new \Exception("Password missing from mongodb config group : {$this->activate}", 500);
            } else {
                $this->password = trim($this->config[$this->activate]['password']);
            }
            if (empty($this->config[$this->activate]['database'])) {
                throw new \Exception("Database name missing from mongodb config group : {$this->activate}", 500);
            } else {
                $this->database = trim($this->config[$this->activate]['database']);
            }
            if (empty($this->config[$this->activate]['db_debug'])) {
                $this->debug = FALSE;
            } else {
                $this->debug = $this->config[$this->activate]['db_debug'];
            }
            if (empty($this->config[$this->activate]['return_as'])) {
                $this->return_as = 'array';
            } else {
                $this->set_return_as($this->config[$this->activate]['return_as']);
                $this->return_as = $this->config[$this->activate]['return_as'];
            }
            if (empty($this->config[$this->activate]['legacy_support'])) {
                $this->legacy_support = false;
            } else {
                $this->legacy_support = $this->config[$this->activate]['legacy_support'];
            }
            if (empty($this->config[$this->activate]['read_preference']) ||
                !isset($this->config[$this->activate]['read_preference'])) {
                $this->read_preference = \MongoDB\Driver\ReadPreference::RP_PRIMARY;
            } else {
                $this->read_preference = $this->config[$this->activate]['read_preference'];
            }
            if (empty($this->config[$this->activate]['read_concern']) ||
                !isset($this->config[$this->activate]['read_concern'])) {
                $this->read_concern = \MongoDB\Driver\ReadConcern::MAJORITY;
            } else {
                $this->read_concern = $this->config[$this->activate]['read_concern'];
            }
        } else {
            throw new \Exception("mongodb config group :  <strong>{$this->activate}</strong> does not exist.", 500);
        }
    }

    /**
     * 设置并检查返回数据类型
     * 建议使用数组方式
     * @param $value
     * @throws \Exception
     */
    private function set_return_as($value)
    {
        if (!in_array($value, ['array', 'object'])) {
            throw new \Exception("Invalid Return As Type");
        }
        $this->return_as = $value;
    }

    /**
     * 连接 MongoDB
     * @throws \Exception
     */
    private function connect()
    {
        $this->prepare();
        try {
            $dns = "mongodb://{$this->hostname}:{$this->port}/{$this->database}";
            if (isset($this->config[$this->activate]['no_auth']) == TRUE && $this->config[$this->activate]['no_auth'] == TRUE) {
                $options = array();
            } else {
                $options = array('username' => $this->username, 'password' => $this->password);
            }
            $this->connect = $this->db = new \MongoDB\Driver\Manager($dns, $options);
        } catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Unable to connect to MongoDB: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Unable to connect to MongoDB", 500);
            }
        }
    }

    /**
     * $this->mongo_db->insert('foo', $data = array());
     * 插入 文档
     * @param string $collection
     * @param array $insert
     * @return mixed
     * @throws \Exception
     */
    public function insert($collection = "", $insert = array())
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected to insert into", 500);
        }
        if (!is_array($insert) || count($insert) == 0) {
            throw new \Exception("Nothing to insert into Mongo collection or insert is not an array", 500);
        }
        if (isset($insert['_id']) === false) {
            $insert['_id'] = new \MongoDB\BSON\ObjectId;
        }
        $bulk = new \MongoDB\Driver\BulkWrite();
        $bulk->insert($insert);
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            $write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
            return $this->convert_document_id($insert);
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();
            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Insert of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Insert of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->batch_insert('foo', $data = array());
     * 批量插入 文档
     * @param string $collection
     * @param array $insert
     * @return mixed
     * @throws \Exception
     */
    public function batch_insert($collection = "", $insert = array())
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected to insert into", 500);
        }
        if (!is_array($insert) || count($insert) == 0) {
            throw new \Exception("Nothing to insert into Mongo collection or insert is not an array", 500);
        }
        $doc = new \MongoDB\Driver\BulkWrite();
        foreach ($insert as $ins) {
            if (isset($ins['_id']) === false) {
                $ins['_id'] = new \MongoDB\BSON\ObjectId;
            }
            $doc->insert($ins);
        }
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            return $this->db->executeBulkWrite($this->database . "." . $collection, $doc, $writeConcern);
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();
            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Insert of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Insert of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->select(array('foo', 'bar'))->get('foobar');
     * 查询包含字段或者不包含字段
     * @param array $includes
     * @param array $excludes
     * @return MongoDB
     */
    public function select($includes = array(), $excludes = array())
    {
        if (!is_array($includes)) {
            $includes = array();
        }
        if (!is_array($excludes)) {
            $excludes = array();
        }
        if (!empty($includes)) {
            foreach ($includes as $key => $col) {
                if (is_array($col)) {
                    //support $elemMatch in select
                    $this->selects[$key] = $col;
                } else {
                    $this->selects[$col] = 1;
                }
            }
        }
        if (!empty($excludes)) {
            foreach ($excludes as $col) {
                $this->selects[$col] = 0;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('foo' => 'bar'))->get('foobar');
     * 检索条件
     * @param $wheres
     * @param null $value
     * @return MongoDB
     */
    public function where($wheres, $value = null)
    {
        if (is_array($wheres)) {
            foreach ($wheres as $wh => $val) {
                $this->wheres[$wh] = $val;
            }
        } else {
            $this->wheres[$wheres] = $value;
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where_or(array('foo'=>'bar', 'bar'=>'foo'))->get('foobar');
     * 检索或条件
     * @param array $wheres
     * @return MongoDB
     * @throws \Exception
     */
    public function where_or($wheres = array())
    {
        if (is_array($wheres) && count($wheres) > 0) {
            if (!isset($this->wheres['$or']) || !is_array($this->wheres['$or'])) {
                $this->wheres['$or'] = array();
            }
            foreach ($wheres as $wh => $val) {
                $this->wheres['$or'][] = array($wh => $val);
            }
            return ($this);
        } else {
            throw new \Exception("Where value should be an array.", 500);
        }
    }

    /**
     * $this->mongo_db->where_in('foo', array('bar', 'zoo', 'blah'))->get('foobar');
     * 检索包含条件
     * @param string $field
     * @param array $in
     * @return MongoDB
     * @throws \Exception
     */
    public function where_in($field = "", $in = array())
    {
        if (empty($field)) {
            throw new \Exception("Mongo field is require to perform where in query.", 500);
        }
        if (is_array($in) && count($in) > 0) {
            $this->_w($field);
            $this->wheres[$field]['$in'] = $in;
            return ($this);
        } else {
            throw new \Exception("in value should be an array.", 500);
        }
    }

    /**
     * $this->mongo_db->where_in_all('foo', array('bar', 'zoo', 'blah'))->get('foobar');
     * 检索包含所有条件
     * @param string $field
     * @param array $in
     * @return MongoDB
     * @throws \Exception
     */
    public function where_in_all($field = "", $in = array())
    {
        if (empty($field)) {
            throw new \Exception("Mongo field is require to perform where all in query.", 500);
        }
        if (is_array($in) && count($in) > 0) {
            $this->_w($field);
            $this->wheres[$field]['$all'] = $in;
            return ($this);
        } else {
            throw new \Exception("in value should be an array.", 500);
        }
    }

    /**
     * $this->mongo_db->where_not_in('foo', array('bar', 'zoo', 'blah'))->get('foobar');
     * 检索不包含条件
     * @param string $field
     * @param array $in
     * @return MongoDB
     * @throws \Exception
     */
    public function where_not_in($field = "", $in = array())
    {
        if (empty($field)) {
            throw new \Exception("Mongo field is require to perform where not in query.", 500);
        }
        if (is_array($in) && count($in) > 0) {
            $this->_w($field);
            $this->wheres[$field]['$nin'] = $in;
            return ($this);
        } else {
            throw new \Exception("in value should be an array.", 500);
        }
    }

    /**
     * $this->mongo_db->where_gt('foo', 20);
     * 检索条件大于
     * @param string $field
     * @param $x
     * @return MongoDB
     * @throws \Exception
     */
    public function where_gt($field = "", $x)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform greater then query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's value is require to perform greater then query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$gt'] = $x;
        return ($this);
    }

    /**
     * $this->mongo_db->where_gte('foo', 20);
     * 检索条件大于等于
     * @param string $field
     * @param $x
     * @return MongoDB
     * @throws \Exception
     */
    public function where_gte($field = "", $x)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform greater then or equal query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's value is require to perform greater then or equal query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$gte'] = $x;
        return ($this);
    }

    /**
     * $this->mongo_db->where_lt('foo', 20);
     * 检索条件小于
     * @param string $field
     * @param $x
     * @return MongoDB
     * @throws \Exception
     */
    public function where_lt($field = "", $x)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform less then query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's value is require to perform less then query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$lt'] = $x;
        return ($this);
    }

    /**
     * $this->mongo_db->where_lte('foo', 20);
     * 检索条件小于等于
     * @param string $field
     * @param $x
     * @return MongoDB
     * @throws \Exception
     */
    public function where_lte($field = "", $x)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform less then or equal to query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's value is require to perform less then or equal to query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$lte'] = $x;
        return ($this);
    }

    /**
     * $this->mongo_db->where_between('foo', 20, 30);
     * 检索条件在两个数值之间
     * @param string $field
     * @param $x
     * @param $y
     * @return MongoDB
     * @throws \Exception
     */
    public function where_between($field = "", $x, $y)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform greater then or equal to query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's start value is require to perform greater then or equal to query.", 500);
        }
        if (!isset($y)) {
            throw new \Exception("Mongo field's end value is require to perform greater then or equal to query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$gte'] = $x;
        $this->wheres[$field]['$lte'] = $y;
        return ($this);
    }

    /**
     * $this->mongo_db->where_between_ne('foo', 20, 30);
     * 检索条件不在在两个数值之间
     * @param string $field
     * @param $x
     * @param $y
     * @return MongoDB
     * @throws \Exception
     */
    public function where_between_ne($field = "", $x, $y)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform between and but not equal to query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's start value is require to perform between and but not equal to query.", 500);
        }
        if (!isset($y)) {
            throw new \Exception("Mongo field's end value is require to perform between and but not equal to query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$gt'] = $x;
        $this->wheres[$field]['$lt'] = $y;
        return ($this);
    }

    /**
     * $this->mongo_db->where_ne('foo', 1)->get('foobar');
     * 检索条件不包含
     * @param string $field
     * @param $x
     * @return MongoDB
     * @throws \Exception
     */
    public function where_ne($field = '', $x)
    {
        if (!isset($field)) {
            throw new \Exception("Mongo field is require to perform Where not equal to query.", 500);
        }
        if (!isset($x)) {
            throw new \Exception("Mongo field's value is require to perform Where not equal to query.", 500);
        }
        $this->_w($field);
        $this->wheres[$field]['$ne'] = $x;
        return ($this);
    }

    /**
     * $this->mongo_db->like('foo', 'bar', 'im', FALSE, TRUE);
     * 模糊查询
     * @param $flags
     * 允许使用典型的正则表达式标志:
     * i = case insensitive
     * m = multiline
     * x = can contain comments
     * l = locale
     * s = dotall, "." matches everything, including newlines
     * u = match unicode
     *
     * @param $enable_start_wildcard
     * If set to anything other than TRUE, a starting line character "^" will be prepended
     * to the search value, representing only searching for a value at the start of
     * a new line.
     *
     * @param $enable_end_wildcard
     * If set to anything other than TRUE, an ending line character "$" will be appended
     * to the search value, representing only searching for a value at the end of
     * a line.
     *
     * @param string $field
     * @param string $value
     * @param string $flags
     * @param bool $enable_start_wildcard
     * @param bool $enable_end_wildcard
     * @return MongoDB
     * @throws \Exception
     */
    public function like($field = "", $value = "", $flags = "i", $enable_start_wildcard = TRUE, $enable_end_wildcard = TRUE)
    {
        if (empty($field)) {
            throw new \Exception("Mongo field is require to perform like query.", 500);
        }

        if (empty($value)) {
            throw new \Exception("Mongo field's value is require to like query.", 500);
        }

        $field = (string)trim($field);
        $this->_w($field);
        $value = (string)trim($value);
        $value = quotemeta($value);
        if ($enable_start_wildcard !== TRUE) {
            $value = "^" . $value;
        }
        if ($enable_end_wildcard !== TRUE) {
            $value .= "$";
        }
        $regex = "/$value/$flags";
        $this->wheres[$field] = new \MongoRegex($regex);
        return ($this);
    }

    /**
     * $this->mongo_db->get('foo');
     * 根据集合名称参数获取相关文档
     * @param string $collection
     * @return array|object
     * @throws \Exception
     */
    public function get($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("In order to retrieve documents from MongoDB, a collection name must be passed", 500);
        }
        try {
            $read_concern = new \MongoDB\Driver\ReadConcern($this->read_concern);
            $read_preference = new \MongoDB\Driver\ReadPreference($this->read_preference);
            $options = array();
            $options['projection'] = $this->selects;
            $options['sort'] = $this->sorts;
            $options['skip'] = (int)$this->offset;
            $options['limit'] = (int)$this->limit;
            $options['readConcern'] = $read_concern;
            $query = new \MongoDB\Driver\Query($this->wheres, $options);
            $cursor = $this->db->executeQuery($this->database . "." . $collection, $query, $read_preference);
            // Clear
            $this->_clear();
            $returns = array();
            if ($cursor instanceof \MongoDB\Driver\Cursor) {
                $it = new \IteratorIterator($cursor);
                $it->rewind();
                while ($doc = (array)$it->current()) {
                    if ($this->return_as == 'object') {
                        $returns[] = (object)$this->convert_document_id($doc);
                    } else {
                        $returns[] = (array)$this->convert_document_id($doc);
                    }
                    $it->next();
                }
            }
            if ($this->return_as == 'object') {
                return (object)$returns;
            } else {
                return $returns;
            }
        } catch (\MongoDB\Driver\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("MongoDB query failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("MongoDB query failed.", 500);
            }
        }
    }

    /**
     * $this->mongo_db->get_where('foo', array('bar' => 'something'));
     * 根据集合名称与字段参数获取相关文档
     * @param string $collection
     * @param array $where
     * @return array|object
     * @throws \Exception
     */
    public function get_where($collection = "", $where = array())
    {
        if (is_array($where) && count($where) > 0) {
            return $this->where($where)->get($collection);
        } else {
            throw new \Exception("Nothing passed to perform search or value is empty.", 500);
        }
    }

    /**
     * $this->mongo_db->find_one('foo');
     * 根据集合名称参数获取单个文档
     * @param string $collection
     * @return array|object
     * @throws \Exception
     */
    public function find_one($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("In order to retrieve documents from MongoDB, a collection name must be passed", 500);
        }
        try {
            $read_concern = new \MongoDB\Driver\ReadConcern($this->read_concern);
            $read_preference = new \MongoDB\Driver\ReadPreference($this->read_preference);
            $options = array();
            $options['projection'] = $this->selects;
            $options['sort'] = $this->sorts;
            $options['skip'] = (int)$this->offset;
            $options['limit'] = (int)1;
            $options['readConcern'] = $read_concern;
            $query = new \MongoDB\Driver\Query($this->wheres, $options);
            $cursor = $this->db->executeQuery($this->database . "." . $collection, $query, $read_preference);
            // Clear
            $this->_clear();
            $returns = array();
            if ($cursor instanceof \MongoDB\Driver\Cursor) {
                $it = new \IteratorIterator($cursor);
                $it->rewind();
                while ($doc = (array)$it->current()) {
                    if ($this->return_as == 'object') {
                        $returns[] = (object)$this->convert_document_id($doc);
                    } else {
                        $returns[] = (array)$this->convert_document_id($doc);
                    }
                    $it->next();
                }
            }
            if ($this->return_as == 'object') {
                return (object)$returns;
            } else {
                return $returns;
            }
        } catch (\MongoDB\Driver\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("MongoDB query failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("MongoDB query failed.", 500);
            }
        }
    }

    /**
     * $this->mongo_db->count('foo');
     * 根据集合名称参数对文档进行计数
     * @param string $collection
     * @return int
     * @throws \Exception
     */
    public function count($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("In order to retrieve documents from MongoDB, a collection name must be passed", 500);
        }
        try {
            $read_concern = new \MongoDB\Driver\ReadConcern($this->read_concern);
            $read_preference = new \MongoDB\Driver\ReadPreference($this->read_preference);
            $options = array();
            $options['projection'] = array('_id' => 1);
            $options['sort'] = $this->sorts;
            $options['skip'] = (int)$this->offset;
            $options['limit'] = (int)$this->limit;
            $options['readConcern'] = $read_concern;
            $query = new \MongoDB\Driver\Query($this->wheres, $options);
            $cursor = $this->db->executeQuery($this->database . "." . $collection, $query, $read_preference);
            $array = $cursor->toArray();
            // Clear
            $this->_clear();
            return count($array);
        } catch (\MongoDB\Driver\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("MongoDB query failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("MongoDB query failed.", 500);
            }
        }
    }

    /**
     * $this->mongo_db->count('foo');
     * 调用count方法 与CI查询生成器兼容的别名计数方法
     * @param string $collection
     * @return int
     * @throws \Exception
     */
    public function count_all_results($collection = "")
    {
        return $this->count($collection);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->set('posted', 1)->update('blog_posts');
     * $this->mongo_db->where(array('blog_id'=>123))->set(array('posted' => 1, 'time' => time()))->update('blog_posts');
     * 设置字段值
     * @param $fields
     * @param null $value
     * @return MongoDB
     */
    public function set($fields, $value = NULL)
    {
        $this->_u('$set');
        if (is_string($fields)) {
            $this->updates['$set'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$set'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->unset('posted')->update('blog_posts');
     * $this->mongo_db->where(array('blog_id'=>123))->set(array('posted','time'))->update('blog_posts');
     * 去除字段（支持多个）
     * @param $fields
     * @return MongoDB
     */
    public function unset_field($fields)
    {
        $this->_u('$unset');
        if (is_string($fields)) {
            $this->updates['$unset'][$fields] = 1;
        } elseif (is_array($fields)) {
            foreach ($fields as $field) {
                $this->updates['$unset'][$field] = 1;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->addToSet('tags', 'php')->update('blog_posts');
     * $this->mongo_db->where(array('blog_id'=>123))->addToSet('tags', array('php', 'codeigniter', 'mongodb'))->update('blog_posts');
     * 仅当数组不在数组中时，才向数组添加值
     * @param $field
     * @param $values
     * @return MongoDB
     */
    public function addToSet($field, $values)
    {
        $this->_u('$addToSet');
        if (is_string($values)) {
            $this->updates['$addToSet'][$field] = $values;
        } elseif (is_array($values)) {
            $this->updates['$addToSet'][$field] = array('$each' => $values);
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->addtoset('tags', 'php')->update('blog_posts');
     * $this->mongo_db->where(array('blog_id'=>123))->addtoset('tags', array('php', 'codeigniter', 'mongodb'))->update('blog_posts');
     * 向数组头部添加
     * @param $fields
     * @param array $value
     * @return MongoDB
     */
    public function push($fields, $value = array())
    {
        $this->_u('$push');
        if (is_string($fields)) {
            $this->updates['$push'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$push'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->pop('comments')->update('blog_posts');
     * $this->mongo_db->where(array('blog_id'=>123))->pop(array('comments', 'viewed_by'))->update('blog_posts');
     * 向数组尾部添加
     * @param $field
     * @return MongoDB
     */
    public function pop($field)
    {
        $this->_u('$pop');
        if (is_string($field)) {
            $this->updates['$pop'][$field] = -1;
        } elseif (is_array($field)) {
            foreach ($field as $pop_field) {
                $this->updates['$pop'][$pop_field] = -1;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->pull('comments', array('comment_id'=>123))->update('blog_posts');
     * 从数组中移除
     * @param string $field
     * @param array $value
     * @return MongoDB
     */
    public function pull($field = "", $value = array())
    {
        $this->_u('$pull');
        $this->updates['$pull'] = array($field => $value);
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->rename_field('posted_by', 'author')->update('blog_posts');
     * 字段重命名
     * @param $old
     * @param $new
     * @return MongoDB
     */
    public function rename_field($old, $new)
    {
        $this->_u('$rename');
        $this->updates['$rename'] = array($old => $new);
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->inc(array('num_comments' => 1))->update('blog_posts');
     * 字段增值
     * @param array $fields
     * @param int $value
     * @return MongoDB
     */
    public function inc($fields = array(), $value = 0)
    {
        $this->_u('$inc');
        if (is_string($fields)) {
            $this->updates['$inc'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$inc'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->mul(array('num_comments' => 3))->update('blog_posts');
     * 字段增倍数
     * @param array $fields
     * @param int $value
     * @return MongoDB
     */
    public function mul($fields = array(), $value = 0)
    {
        $this->_u('$mul');
        if (is_string($fields)) {
            $this->updates['$mul'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$mul'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->max(array('num_comments' => 3))->update('blog_posts');
     * 如果指定的值大于字段的当前值，$max运算符会将该字段的值更新为指定值。
     * @param array $fields
     * @param int $value
     * @return MongoDB
     */
    public function max($fields = array(), $value = 0)
    {
        $this->_u('$max');
        if (is_string($fields)) {
            $this->updates['$max'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$max'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->where(array('blog_id'=>123))->min(array('num_comments' => 3))->update('blog_posts');
     * 如果指定的值小于字段的当前值，$min会将该字段的值更新为指定值。
     * @param array $fields
     * @param int $value
     * @return MongoDB
     */
    public function min($fields = array(), $value = 0)
    {
        $this->_u('$min');
        if (is_string($fields)) {
            $this->updates['$min'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$min'][$field] = $value;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->distinct('collection', 'field');
     * 在单个字段中查找指定的非重复值
     * @param string $collection
     * @param string $field
     * @return object
     * @throws \Exception
     */
    public function distinct($collection = "", $field = "")
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected for update", 500);
        }

        if (empty($field)) {
            throw new \Exception("Need Collection field information for performing distinct query", 500);
        }

        try {
            $documents = $this->db->{$collection}->distinct($field, $this->wheres);
            $this->_clear();
            if ($this->return_as == 'object') {
                return (object)$documents;
            } else {
                return $documents;
            }
        } catch (\MongoCursorException $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("MongoDB Distinct Query Failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->update('foo', $data = array());
     * 更新单个文档
     * @param string $collection
     * @param array $options
     * @return mixed
     * @throws \Exception
     */
    public function update($collection = "", $options = array())
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected for update", 500);
        }
        $bulk = new \MongoDB\Driver\BulkWrite();
        $bulk->update($this->wheres, $this->updates, $options);
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            $write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
            $this->_clear();
            return $write;
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();
            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Update of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Update of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->update_all('foo', $data = array());
     * 更新多个文档
     * @param string $collection
     * @param array $data
     * @param array $options
     * @return mixed
     * @throws \Exception
     */
    public function update_all($collection = "", $data = array(), $options = array())
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected to update", 500);
        }
        if (is_array($data) && count($data) > 0) {
            $this->updates = array_merge($data, $this->updates);
        }
        if (count($this->updates) == 0) {
            throw new \Exception("Nothing to update in Mongo collection or update is not an array", 500);
        }
        $options = array_merge(array('multi' => TRUE), $options);
        $bulk = new \MongoDB\Driver\BulkWrite();
        $bulk->update($this->wheres, $this->updates, $options);
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            $write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
            $this->_clear();
            return $write;
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();

            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Update of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Update of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->delete('foo');
     * 删除单个文档
     * @param string $collection
     * @return mixed
     * @throws \Exception
     */
    public function delete($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected for update", 500);
        }
        $options = array('limit' => true);
        $bulk = new \MongoDB\Driver\BulkWrite();
        $bulk->delete($this->wheres, $options);
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            $write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
            $this->_clear();
            return $write;
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();
            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Update of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Update of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->delete_all('foo', $data = array());
     * 删除多个文档
     * @param string $collection
     * @return mixed
     * @throws \Exception
     */
    public function delete_all($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection selected for delete", 500);
        }
        $options = array('limit' => false);
        $bulk = new \MongoDB\Driver\BulkWrite();
        $bulk->delete($this->wheres, $options);
        $writeConcern = new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY, 1000);
        try {
            $write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
            $this->_clear();
            return $write;
        } // Check if the write concern could not be fulfilled
        catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $result = $e->getWriteResult();
            if ($writeConcernError = $result->getWriteConcernError()) {
                if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                    throw new \Exception("WriteConcern failure : {$writeConcernError->getMessage()}", 500);
                } else {
                    throw new \Exception("WriteConcern failure", 500);
                }
            }
        } // Check if any general error occured.
        catch (\MongoDB\Driver\Exception\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("Delete of data into MongoDB failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("Delete of data into MongoDB failed", 500);
            }
        }
    }

    /**
     * $this->mongo_db->aggregate('foo', $ops = array());
     * 执行聚合
     * @param $collection
     * @param $operation
     * @return array|object
     * @throws \Exception
     */
    public function aggregate($collection, $operation)
    {
        if (empty($collection)) {
            throw new \Exception("In order to retreive documents from MongoDB, a collection name must be passed", 500);
        }
        if (empty($operation) && !is_array($operation)) {
            throw new \Exception("Operation must be an array to perform aggregate.", 500);
        }
        $command = array('aggregate' => $collection, 'pipeline' => $operation);
        return $this->command($command);
    }

    /**
     * $this->mongo_db->order_by(array('foo' => 'ASC'))->get('foobar');
     * 根据传递的参数对文档进行排序。要按降序设置值必须传递-1、FALSE、“desc”或“desc”的值，否则它们将是设置为1（ASC）
     * @param array $fields
     * @return MongoDB
     */
    public function order_by($fields = array())
    {
        foreach ($fields as $col => $val) {
            if ($val == -1 || $val === FALSE || strtolower($val) == 'desc') {
                $this->sorts[$col] = -1;
            } else {
                $this->sorts[$col] = 1;
            }
        }
        return ($this);
    }

    /**
     * $this->mongo_db->date($timestamp);
     * 从当前时间创建新的MongoDate对象或传递时间戳以创建MongoDate
     * @param bool $stamp
     * @return \MongoDB\BSON\UTCDateTime
     */
    public function date($stamp = FALSE)
    {
        if ($stamp == FALSE) {
            return new \MongoDB\BSON\UTCDateTime();
        } else {
            return new \MongoDB\BSON\UTCDateTime($stamp);
        }
    }

    /**
     * $this->mongo_db->limit($x);
     * 将结果集限制为$x文档数
     * @param int $x
     * @return MongoDB
     */
    public function limit($x = 99999)
    {
        if ($x !== NULL && is_numeric($x) && $x >= 1) {
            $this->limit = (int)$x;
        }
        return ($this);
    }

    /**
     * $this->mongo_db->offset($x);
     * 偏移结果集以跳过$x个文档数
     * @param int $x
     * @return MongoDB
     */
    public function offset($x = 0)
    {
        if ($x !== NULL && is_numeric($x) && $x >= 1) {
            $this->offset = (int)$x;
        }
        return ($this);
    }

    /**
     * 转化文档_id
     * @param $document
     * @return mixed
     */
    private function convert_document_id($document)
    {
        if ($this->legacy_support === TRUE && isset($document['_id']) && $document['_id'] instanceof \MongoDB\BSON\ObjectId) {
            $new_id = $document['_id']->__toString();
            unset($document['_id']);
            $document['_id'] = new \stdClass();
            $document['_id']->{'$id'} = $new_id;
        }
        return $document;
    }

    /**
     * $this->mongo_db->command($collection, array('geoNear'=>'buildings', 'near'=>array(53.228482, -0.547847), 'num' => 10, 'nearSphere'=>true));
     * 运行MongoDB命令
     * @param array $command
     * @return array|object
     * @throws \Exception
     */
    public function command($command = array())
    {
        try {
            $cursor = $this->db->executeCommand($this->database, new \MongoDB\Driver\Command($command));
            // Clear
            $this->_clear();
            $returns = array();
            if ($cursor instanceof \MongoDB\Driver\Cursor) {
                $it = new \IteratorIterator($cursor);
                $it->rewind();
                while ($doc = (array)$it->current()) {
                    if ($this->return_as == 'object') {
                        $returns[] = (object)$this->convert_document_id($doc);
                    } else {
                        $returns[] = (array)$this->convert_document_id($doc);
                    }
                    $it->next();
                }
            }
            if ($this->return_as == 'object') {
                return (object)$returns;
            } else {
                return $returns;
            }
        } catch (\MongoDB\Driver\Exception $e) {
            if (isset($this->debug) == TRUE && $this->debug == TRUE) {
                throw new \Exception("MongoDB query failed: {$e->getMessage()}", 500);
            } else {
                throw new \Exception("MongoDB query failed.", 500);
            }
        }
    }

    /**
     * $this->mongo_db->add_index($collection, array('first_name' => 'ASC', 'last_name' => -1), array('unique' => TRUE));
     * 添加索引
     * @param string $collection
     * @param array $keys
     * @param array $options
     * @return array|object
     * @throws \Exception
     */
    public function add_index($collection = "", $keys = array(), $options = array())
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection specified to add index to", 500);
        }
        if (empty($keys) || !is_array($keys)) {
            throw new \Exception("Index could not be created to MongoDB Collection because no keys were specified", 500);
        }
        foreach ($keys as $col => $val) {
            if ($val == -1 || $val === FALSE || strtolower($val) == 'desc') {
                $keys[$col] = -1;
            } else {
                $keys[$col] = 1;
            }
        }
        $command = array();
        $command['createIndexes'] = $collection;
        $command['indexes'] = array($keys);
        return $this->command($command);
    }

    /**
     * $this->mongo_db->remove_index($collection, 'index_1');
     * 删除索引
     * @param string $collection
     * @param string $name
     * @return array|object
     * @throws \Exception
     */
    public function remove_index($collection = "", $name = "")
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection specified to remove index from", 500);
        }
        if (empty($keys)) {
            throw new \Exception("Index could not be removed from MongoDB Collection because no index name were specified", 500);
        }
        $command = array();
        $command['dropIndexes'] = $collection;
        $command['index'] = $name;
        return $this->command($command);
    }

    /**
     * $this->mongo_db->list_indexes($collection);
     * 获取索引
     * @param string $collection
     * @return array|object
     * @throws \Exception
     */
    public function list_indexes($collection = "")
    {
        if (empty($collection)) {
            throw new \Exception("No Mongo collection specified to list all indexes from", 500);
        }
        $command = array();
        $command['listIndexes'] = $collection;
        return $this->command($command);
    }

    /**
     * $this->mongo_db->drop_db("foobar");
     * 删除数据库
     * @param string $database
     * @return array|object
     * @throws \Exception
     */
    public function drop_db($database = '')
    {
        if (empty($database)) {
            throw new \Exception('Failed to drop MongoDB database because name is empty', 500);
        }
        $command = array();
        $command['dropDatabase'] = 1;
        return $this->command($command);
    }

    /**
     * $this->mongo_db->drop_collection('bar');
     * 删除集合
     * @param string $col
     * @return array|object
     * @throws \Exception
     */
    public function drop_collection($col = '')
    {
        if (empty($col)) {
            throw new \Exception('Failed to drop MongoDB collection because collection name is empty', 500);
        }
        $command = array();
        $command['drop'] = $col;
        return $this->command($command);
    }

    /**
     * 将类变量重置为默认设置
     */
    private function _clear()
    {
        $this->selects = array();
        $this->updates = array();
        $this->wheres = array();
        $this->limit = 999999;
        $this->offset = 0;
        $this->sorts = array();
    }

    /**
     * 向条件参数中添加参数
     * @param $param
     */
    private function _w($param)
    {
        if (!isset($this->wheres[$param])) {
            $this->wheres[$param] = array();
        }
    }

    /**
     * 向更新参数中添加参数
     * @param $method
     */
    private function _u($method)
    {
        if (!isset($this->updates[$method])) {
            $this->updates[$method] = array();
        }
    }
}
