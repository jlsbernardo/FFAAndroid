<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Territory extends DB
{
    protected $ffaTblName = 'tbl_area_structure';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'geo_hierarchy_territory';

    protected $reportTable = 'geo_hierarchy_territory';

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

        parent::__construct();
    }

    public function getStaging($last_insert_id = '')
    {

        $sql = "SELECT
            ffa_id,
            level,
            caption
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        ORDER by create_on DESC";

        $territoryInsertQuery = "";
        $result = $this->exec_query($sql);
        $territoryData = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {

                $ffa_id = $row['ffa_id'];
                $last_insert_id = '';

                $territoryRNAFields = array(
                    'ffa_id'   => $row['ffa_id'],
                    'deleted'  => 0,
                    'caption'  => str_replace("'", "",$row['caption']),
                    'country'  => $this->country['country_name'],
                    'level'    => $row['level']
                );

                $territoryData[] = $territoryRNAFields;
            }

            $territory = $this->__getTerritoryData($territoryData);
            $message = "Territory records now synced to analytical territory table. ";

            return [
                'num_rows'          => $territory['count'],
                'last_insert_id'    => $territory['last_inserted'],
                'message'           => $message
            ];
        } else {
            $message = "No Zone Records to sync";
            //echo $message;

            return [
                'message'   => $message
            ];
        }
    }

    private function __getTerritoryData($territoryData)
    { 
        $lastInserted = '';
        $countTerritory = 0;
        foreach ($territoryData as $data)
        {
            // check the record if existing
            $check_record = $this->__checkTerritoryRecord($data['ffa_id']);
            $ffa_id = $data['ffa_id'];

            if (!is_array($check_record)) {
                $strColumns = implode(', ', array_keys($data));
                $strValues =  " '" . implode("', '", array_values($data)) . "' ";
                $territoryInsertQuery = "INSERT INTO [$this->schemaName].[$this->analyticalTable]  ({$strColumns}) VALUES ({$strValues});";
                $result = $this->exec_query($territoryInsertQuery);

                if ($result) {
                    $countTerritory += 1;
                }

                $lastInserted = $ffa_id;
            } else {

                if ($check_record['level'] != $data['level'] || $check_record['caption'] != $data['caption']) {
                    $country = $this->country['country_name'];
                    $territoryUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->analyticalTable] 
                        SET 
                            caption = '{$data['caption']}',
                            level   = '{$data['level']}'
                        WHERE [ffa_id] = '$ffa_id'
                        AND country = '$country';";
                    $res = $this->exec_query($territoryUpdateQuery);
                    
                    if ($res !== false || $res != -1) {
                        $countTerritory += 1;
                    }
                }
            }
        }

        return [
            'count' => $countTerritory,
            'last_inserted' => $lastInserted
        ];
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


    private function __checkTerritoryRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id, level, caption
        FROM [$this->schemaName].[$this->analyticalTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

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
