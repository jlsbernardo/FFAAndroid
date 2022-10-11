<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class TargetActivity extends DB
{

    protected $ffaTable = 'tbl_target_activities';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'target_activities';

    protected $reportTable = 'target_activities';

    protected $country = '';

    protected $connection = '';

    protected $last_insert_id;

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa', $last_insert_id = '')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;
        $this->last_insert_id = $last_insert_id;

        parent::__construct();
    }

    public function getStagingETL()
    {   
        // $sql_filter = "";
        // if ($this->last_insert_id) {
        //     $sql_filter .= " AND ffa_id > {$this->last_insert_id} ";
        // }
        $sql = "SELECT
            [ffa_id],
            [module],
            [type],
            [key],
            [value],
            [month],
            [create_on],
            [created_by],
            [update_on],
            [update_by],
            [activity_type]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE 
            report_table = '$this->reportTable'";

        $portSettingsInsertQuery = "";
        $result = $this->exec_query($sql);

        if (sqlsrv_num_rows($result) > 0) {
            $portalSettingData = [];
            $countPortalSettings = 0;
            while ($row = sqlsrv_fetch_array($result)) {
                $key = $row['key'];
                $value = $row['value'];
                $ffa_id = $row['ffa_id'];
                $last_insert_id = '';

                $country = $this->country['country_name'];
                $check_record = $this->__checkPortalSettingRecord($row['ffa_id']);
                $updated_on = $row['update_on'] != 0 ? convert_to_datetime($row['update_on']) : NULL;
                $created_on = convert_to_datetime($row['create_on']);

                if (!is_array($check_record)) {
                    // insert data with the normalized value from staging tables
                    $portalRNAFields = array(
                        'ffa_id'      => $ffa_id,
                        'deleted'     => 0,
                        'module'      => $row['module'],
                        'month'         => $row['month'],
                        'type'      => $row['type'],
                        '[key]'       => $key,
                        'value'     => $value,
                        'country'     => $this->country['country_name'],
                        'create_on' => $created_on,
                        'create_by' => $row['created_by'],
                        'update_on' => $updated_on,
                        'update_by' => $row['update_by'],
                        'activity_type'=> $row['activity_type']
                    );

                    $portalSettingData[] = $portalRNAFields;
                } else {
                    // Update data with the normalized value from staging tables
                    $month = date("M-Y");
                    $portSettingsQuery = "
                        UPDATE [$this->schemaName].[$this->analyticalTable] 
                        SET
                            [module] = '{$row['module']}',
                            [type]   = '{$row['type']}',
                            [key]  = '{$key}',
                            [value]  = '{$value}',
                            [create_on] = '{$created_on}',
                            [create_by] = '{$row['created_by']}',
                            [update_on] = '{$updated_on}',
                            [update_by] = '{$row['update_by']}',
                            [activity_type] = '{$row['activity_type']}'
                        WHERE [ffa_id] = '$ffa_id'
                        AND [month] = '$month'
                        AND country = '$country';";
                    $res = $this->exec_query($portSettingsQuery);
                    if ($res) {
                        $countPortalSettings += 1;
                    }
                }
            }


            foreach ($portalSettingData as $data) {
                $strColumns = implode(', ', array_keys($data));
                $strValues =  " '" . implode("', '", array_values($data)) . "' ";
                $portSettingsInsertQuery = "INSERT INTO [$this->schemaName].[$this->analyticalTable] ({$strColumns}) VALUES ({$strValues});";
                $last_insert_id = $ffa_id;
                $result = $this->exec_query($portSettingsInsertQuery);

                if ($result) {
                    $countPortalSettings += 1;
                }
            }

            return [
                'num_rows'      => $countPortalSettings,
                'last_insert_id'=> $last_insert_id
            ];
        } else {
            $message = "No Portal Setting Records to sync";
            return $message;
        }
    }

    public function updateTblRnaEtlSyncFFA($data)
    {    
        $last_insert_id = $data['last_insert_id'];
        $count = $data['num_rows'];

        $check_record = $this->__checkRecordFFASync();
        $action = !$check_record ? 'create' : 'update';
        $current_timestamp = date('Y-m-d H:i:s');
        if ($last_insert_id) {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) VALUES ('{$action}', 'portal_settings', '{$current_timestamp}', 'done', $count, $last_insert_id);";
            $this->insert_query($insert);
        }
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

    private function __checkPortalSettingRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $month = date("M-Y");
        $sql = "SELECT TOP 1 id
        FROM [$this->schemaName].[$this->analyticalTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country' AND [month] = '$month' ";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }
}
