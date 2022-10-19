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

    public function getDataFromFFA()
    {
        $checkLastRecord = $this->__checkLastRecordFFASync();
        $lastInserted = ($checkLastRecord) ? $checkLastRecord['last_insert_id'] : null;

        $sql = "SELECT
            id,
            first_name,
            last_name,
            email,
            uterritory,
            uzone,
            uregion,
            company,
            team,
            active,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(created_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as created_on
        FROM
            $this->ffaTable 
        order by
            created_on
        desc";

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $objVN = new vn_charset_conversion();
                $fn = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', str_replace("'", "",$row['first_name'])) ;
                $ln = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', str_replace("'", "",$row['last_name'])) ;
                $fname = (strtolower($country)=='vietnam') ? $objVN->convert(    $fn    ) : $fn;
                $lname = (strtolower($country)=='vietnam') ? $objVN->convert(    $ln    ) : $ln;
                $team = (strtolower($country)=='vietnam') ? $objVN->convert($row['team']) : $row['team'];
                $usersRNAFields = array(
                    'ffa_id'        => $row['id'],
                    'deleted'       => 0,
                    'report_table'  => $this->reportTable,
                    'first_name'    => $fname,
                    'last_name'     => $lname,
                    'company'       => $row['company'],
                    'email'         => $row['email'],
                    'create_on'     => $row['created_on'],
                    'territory'     => $row['uterritory'],
                    'zone'          => $row['uzone'],
                    'region'        => $row['uregion'],
                    'user_isactive' => $row['active'],
                    'team'          => (strtolower($country)=='thailand') ? $team : trim($team)
                );

                $data[] = $usersRNAFields;
            }

            return [
                'data' => $data,
                'last_inserted' => $lastInserted
            ];

        } else {
            $message = "No User Records to sync";
            return [
                'data' => $message
            ];
        }
    }

    /**
     * Insert to staging table
     *
     * @param $data
     * @return array
     */
    public function insertIntoStaging($data, $lastInserted = null)
    {
        $count = 0;
        $objVN = new vn_charset_conversion();
        foreach ($data as $userRNAFields) {
            if (!empty($userRNAFields['create_on']) && !is_null($userRNAFields['create_on'])) {
                $results = $this->__checkRecordStaging($userRNAFields);

                if (!is_array($results)) {
    
                    // if (date('Y-m-d') == convert_to_datetime($userRNAFields['created_on'], 'Y-m-d')) {
                        if (!is_null($lastInserted)) {

                            // if ($lastInserted > $userRNAFields['ffa_id']) {
                                $strColumns = implode(', ', array_keys($userRNAFields));
                                $strValues =  " '" . implode("', '", array_values($userRNAFields)) . "' ";
                                $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";
                
                                $result = $this->exec_query($userInsertQuery);
                                if ($result) {
                                    $count += 1;
                                }
                            // }
                        } else {
                            $strColumns = implode(', ', array_keys($userRNAFields));
                            $strValues =  " '" . implode("', '", array_values($userRNAFields)) . "' ";
                            $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";
            
                            $result = $this->exec_query($userInsertQuery);
                            if ($result) {
                                $count += 1;
                            }
                        }
                    // }

                } else {
                    $team = (strtolower($country)=='vietnam') ? $objVN->convert( $userRNAFields['team']   ) : $userRNAFields['team'];
                    $team = (strtolower($country)=='thailand') ? $team : trim($team);
                    if ($userRNAFields['first_name'] != $results['first_name'] || $userRNAFields['last_name'] != $results['last_name'] || 
                        $userRNAFields['email'] != $results['email'] || $userRNAFields['company'] != $results['company'] ||
                        $team != $results['team'] || $userRNAFields['territory'] != $results['territory'] ||
                        $userRNAFields['zone'] != $results['zone'] || $userRNAFields['region'] != $results['region']
                    ) {
                        $ffaId = $userRNAFields['ffa_id'];
                        $userUpdateQuery = "
                                UPDATE [$this->schemaName].[$this->stagingTable] 
                                SET 
                                    [first_name] = '{$userRNAFields['first_name']}',
                                    [last_name]  = '{$userRNAFields['last_name']}',
                                    [email]      = '{$userRNAFields['email']}',
                                    [company]    = '{$userRNAFields['company']}',
                                    [team]       = '{$team}',
                                    [territory]  = '{$userRNAFields['territory']}',
                                    [zone]       = '{$userRNAFields['zone']}',
                                    [region]     = '{$userRNAFields['region']}' 
                            WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";
        
                        $result =  $this->exec_query($userUpdateQuery);
                        if ($result !== false || $result != -1) {
                            $count += 1;
                        }
                    }
                }
            }
        }

        return [
            'count' => $count
        ];
    }

    /**
     * Check last record in ffa 
     *
     * @return boolean | array
     */
    private function __checkLastRecordFFASync()
    {
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' AND action_name = 'create' ORDER BY id desc LIMIT 1";
        $results = $this->exec_query($sql);
        
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging($data)
    {
        $ffaId = $data['ffa_id'];
        $sql = "SELECT TOP 1 
            ffa_id, 
            report_table,
            first_name,
            last_name,
            email,
            territory,
            zone,
            region,
            company,
            team
        FROM [$this->schemaName].[$this->stagingTable] 
        WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' 
        ORDER BY id DESC";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }

}
