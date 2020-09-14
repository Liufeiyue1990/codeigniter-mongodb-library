# codeigniter-mongodb-library
推荐PHP7.0以上、codeigniter4.0以上、mongodb 3.0 以上版本。 

推荐新增一个核心数据模型类\app\Core\CoreModel.php 单个集合数据模型类集成核心类。

数据模型加载MongoDB数据库
\app\Models\UserModel.php

//引入数据库类
use App\Libraries\MongoDB;

//连接数据库
//可采用传递参数方式 new MongoDB(array('activate'=>'test')) 切换选择数据库
$this->mongo_db = new MongoDB();

//查询数据
public function find($_id = '')
{
    $this->mongo_db->where('_id', $_id);
    $result = $this->mongo_db->find_one($this->collection_name);
    if ($result) {
        return $result[0];
    }
    return null;
}

//控制器调用查询
\app\Controllers\Home.php
//引入数据模型类
use App\Models\UserModel;
//
$userModel = new UserModel();
$user = $userModel->find('5f5596aa5277b415940016e7');
