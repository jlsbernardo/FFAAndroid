<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class User extends DB
{

    protected $ffaTable = 'users';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';
    
    protected $stagingTable = '';

    protected $analyticalTable = 'users';

    protected $reportTable = 'users';

    protected $country = '';

    protected $connection;

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;

        date_default_timezone_set("Asia/Taipei");
        parent::__construct();
    }

    public function getStagingETL($last_insert_id = '')
    {   
        // $sql_filter = $last_insert_id != '' ? "AND ffa_id > {$last_insert_id}" : "";
        $sql = "SELECT 
            ffa_id,
            first_name,
            last_name,
            email,
            territory,
            zone,
            region,
            company,
            user_isactive,
            team
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        ORDER BY
            create_on DESC";

        $result = $this->exec_query($sql);
        $userInsertQuery = "";
        $countUser = 0;
        $userData = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $ffa_id = $row['ffa_id'];

                $usersRNAFields = array(
                    'ffa_id'      => $ffa_id,
                    'deleted'     => 0,
                    'country'     => $this->country['country_name'],
                    'first_name'  => $row['first_name'],
                    'last_name'   => $row['last_name'],
                    'email'       => $row['email'],
                    'company'     => $row['company'],
                    'team'        => $row['team'],
                    'territory'   => $row['territory'],
                    'zone'        => $row['zone'],
                    'isactive'    => $row['user_isactive'],
                    'region'      => $row['region']
                );

                // check the record if existing
                $userData[] = $usersRNAFields;
            }

            foreach ($userData as $data) {
                $lastInserted = $data['ffa_id'];
                $check_record = $this->__checkUserRecord($data['ffa_id']);

                if (!is_array($check_record)) {
                    $strColumns = implode(', ', array_keys($data));
                    $strValues =  " '" . implode("', '", array_values($data)) . "' ";
                
                    $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->analyticalTable] ({$strColumns}) VALUES ({$strValues});";
                    $result = $this->exec_query($userInsertQuery);

                    if ($result) {
                        $countUser += 1;
                    }

                } else {

                    if ($check_record['first_name'] != $data['first_name'] || $check_record['last_name'] != $data['last_name']
                        || $check_record['email'] != $data['email'] || $check_record['company'] != $data['company']
                        || $check_record['team'] != $data['team'] || $check_record['territory'] != $data['territory'] 
                        || $check_record['zone'] != $data['zone'] || $check_record['region'] != $data['region']
                    ) { 
                        $country = $this->country['country_name'];
                        $userUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->analyticalTable]
                        SET 
                            first_name= '{$data['first_name']}',
                            last_name = '{$data['last_name']}',
                            email     = '{$data['email']}',
                            company   = '{$data['company']}',
                            team      = '{$data['team']}',
                            territory = '{$data['territory']}',
                            zone      = '{$data['zone']}',
                            isactive  = '{$data['isactive']}',
                            region    = '{$data['region']}' 
                        WHERE [ffa_id] = '{$data['ffa_id']}'
                        AND country = '$country';";
                        $result = $this->exec_query($userUpdateQuery);

                        if ($result) {
                            $countUser += 1;
                        }
                    }
                }
            }

            return [
                'num_rows'          => $countUser,
                'last_insert_id'    => $lastInserted,
                'message'           => "User records now synced to analytical user table."
            ];

        } else {
            
            $message = "No User Records to sync";
            return [ 
                'message'           => $message
            ];
        }
    }
    
    public function updateTblRnaEtlSyncFFA($data)
    {    
        $last_insert_id = isset($data['last_insert_id']) ? $data['last_insert_id'] : '';
        $count = isset($data['last_insert_id']) ? $data['num_rows'] : '';

        $check_record = $this->__checkRecordFFASync();
        $action = !$check_record ? 'create' : 'update';
        $current_timestamp = date('Y-m-d H:i:s');
        if ($last_insert_id) {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) VALUES ('{$action}', '$this->reportTable', '{$current_timestamp}', 'done', $count, $last_insert_id);";
            $this->insert_query($insert);
        }
    }

    private function __checkUserRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1
            ffa_id,
            first_name,
            last_name,
            email,
            territory,
            zone,
            region,
            company,
            team
        FROM [$this->schemaName].[$this->analyticalTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country' ";

        $results = $this->exec_query($sql);
        
        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }

    // Check the user record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync()
    {
        $sql = "SELECT
            id,
            last_insert_id,
            last_synced_date
        FROM
            $this->ffaSyncTable
        WHERE
            module = '$this->reportTable' 
        ORDER BY id DESC
        LIMIT 1";  

        $results = $this->exec_query($sql);

        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }

        return false;
    }

}
