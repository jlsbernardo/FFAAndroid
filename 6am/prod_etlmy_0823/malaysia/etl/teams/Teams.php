<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Teams extends DB
{

    protected $ffaTable = 'defaultvalues';

    protected $stagingTable = '';

    protected $analyticalTable = 'teams';

    protected $reportTable = 'teams';

    protected $country = '';

    protected $connection = '';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;
        parent::__construct();
    }

    public function getStaging()
    {
        $sql = "SELECT 
            [ffa_id],
            [module],
            [type],
            [key],
            [value]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE 
            report_table = '$this->reportTable'";

        $portSettingsInsertQuery = "";
        $result = $this->exec_query($sql);

        $countTeamAffectedRows = 0;
        if (sqlsrv_num_rows($result) > 0) {
            $data = [];
            while ($row = sqlsrv_fetch_array($result)) {
                
                $values = explode(';', $row['value']);

                foreach ($values as $value) {
                    $portalRNAFields = array(
                        'ffa_id'    => $row['ffa_id'],
                        'name'      => (strtolower($country)=='thailand') ? $value : trim($value),
                        'country'      => $this->country['country_name'],
                    );

                    $data[] = $portalRNAFields;
                }
            }

            foreach ($data as $teamData) {

                $lastInserted = $teamData['ffa_id'];
                $check_record = $this->__checkTeamRecord($lastInserted, $teamData['name']);

                if (!$check_record) {
                    
                    $strColumns = implode(', ', array_keys($teamData));
                    $strValues =  " '" . implode("', '", array_values($teamData)) . "' ";
                    $portSettingsInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues});";

                    $result =  $this->exec_query($portSettingsInsertQuery);
                    if ($result) {
                        $countTeamAffectedRows  += 1;
                    }
                    
                } else {
                    $teamName = (strtolower($country)=='thailand') ? $teamData['name'] : trim($teamData['name']);
                    $teamUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->reportTable] 
                            SET 
                            name= '{$teamName}'
                        WHERE [ffa_id] = '$lastInserted' AND [name] = '$teamName'";
                    $result =  $this->exec_query($teamUpdateQuery);
                    if ($result) {
                        $countTeamAffectedRows += 1;
                    }
                }
            }

            return [
                'num_rows'    => $countTeamAffectedRows,
                'last_insert_id'  => $lastInserted
            ];

        } else {
            $message = "No Teams Records to sync";
            return $message;
        }
    }

    /**
     * Update tbl_rna_etl_sync in FFA Database
     *
     * @param $lastInserted
     * @param $count
     * 
     */
    public function updateRNAEtlSync($lastInserted, $count) 
    {
        $currentDateTime = date('Y-m-d H:i:s');
        $checkRecords = $this->__checkRecordFFASync($lastInserted);
        $action = !$checkRecords ? 'create' : 'update';

        if ($action == 'create') {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('create', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        } else {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('update', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        }
    }

    // Check the teams record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync($lastInserted)
    {
        $where = (!is_null($lastInserted) && $lastInserted != '') ? 'AND last_insert_id = ' . $lastInserted : '';
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' $where LIMIT 1";
        
        $results = $this->exec_query($sql);
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }

    private function __checkTeamRecord($ffa_id, $value)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id
        FROM [$this->schemaName].[$this->reportTable]
        WHERE [ffa_id] = '$ffa_id' AND [name] = '$value' AND [country] ='$country' ";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }
}
