<?php

class DB
{
    /**
     * Database name
     *
     * @var string
     */
    public string $database = '';

    /**
     * Database username
     *
     * @var string
     */
    public string $username = 'root';

    /**
     * Database password
     *
     * @var string
     */
    public string $password = '';


    /**
     * Database hostname
     *
     * @var string
     */
    public string $hostname = 'localhost';

    // protected $connection = '';

    /**
     * Open Connection
     *
     * @var
     */
    public $db;

    public function __construct()
    {
        $this->dbconnect();
    }

    public function __destruct()
    {
        $this->database = '';
        $this->username = 'root';
        $this->password = '';
        $this->hostname = 'localhost';

        mysqli_close($this->db);
    }

    /**
     * Database Connection
     *
     * @return 
     */
    public function dbconnect()
    {
        include dirname(__DIR__, 3).'/config/database.php';

        if ($this->connection == 'analytical') {
            $this->database = $databases['analytical']['database'];
            $this->username = $databases['analytical']['username'];
            $this->password = $databases['analytical']['password'];
            $this->hostname = $databases['analytical']['hostname'];
        } else {
            foreach ($databases as $key => $database) {
                if ($key == $this->country['short_name'] && $key != 'analytical') {
                    $this->database = $database['ffa_connections']['database'];
                    $this->username = $database['ffa_connections']['username'];
                    $this->password = $database['ffa_connections']['password'];
                    $this->hostname = $database['ffa_connections']['hostname'];
                }
            }
        }

        $this->db = new mysqli($this->hostname, $this->username, $this->password, $this->database);

        /**
         * Check Connection
         */
        if (mysqli_connect_errno()) {
            $message = "Connection failed: " . mysqli_connect_error() . "\n" . "PHP Error: " . new Exception();
            echo Logs::error($message);
            exit();
        }

        return $this;
    }

    /**
     * Execute Queries
     *
     * @param $sql
     * @return mysqli_query
     */
    public function exec_query($sql)
    {
        $query = $this->db->query($sql);

        if (!mysqli_query($this->db, $sql)) {
            echo Logs::error("Mysql Error: " . $this->db->error . "\n" . "Query: " . trim(preg_replace('/\s+/', ' ', trim($sql))) . "\n" . "PHP Error: " . new Exception());
        }

        return $query;
    }

    public function insert_query($sql)
    {
        $query = $this->db->prepare($sql);
        $query->execute();

        if ($query->affected_rows) {
            return $query;
        }

        if (!mysqli_query($this->db, $sql)) {
            echo Logs::error("Mysql Error: " . $this->db->error . "\n" . "Query: " . trim(preg_replace('/\s+/', ' ', trim($sql))) . "\n" . "PHP Error: " . new Exception());
        }

    }
    
}
