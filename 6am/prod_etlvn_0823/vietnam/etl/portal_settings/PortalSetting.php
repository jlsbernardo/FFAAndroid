<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class PortalSetting extends DB
{

    protected $ffaTable = 'defaultvalues';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'portal_settings';

    protected $reportTable = 'portal_settings';

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
        $sql = "SELECT
            [ffa_id],
            [module],
            [type],
            [key],
            [value],
            [update_on],
            [update_by]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE 
            report_table = '$this->reportTable'";

        $portSettingsInsertQuery = "";
        $result = $this->exec_query($sql);

        // List the key modules that has array of values
        $key_array_values = [
            'meeting-types',
            'minimum-execution-time-limit-demo',
            'minimum-execution-time-limit-meeting',
            'minimum-execution-time-limit-farmer-visit',
            'working-hours-demo',
            'working-hours-meeting',
            'working-hours-retailer'
        ];

        if (sqlsrv_num_rows($result) > 0) {
            $portalSettingData = [];
            $countPortalSettings = 0;
            while ($row = sqlsrv_fetch_array($result)) {
                $key = $row['key'];
                $value = $row['value'];
                $ffa_id = $row['ffa_id'];
                $last_insert_id = '';
                $module = $this->__get_ffa_module($key);

                $check_record = $this->__checkPortalSettingRecord($row['ffa_id']);
                $updated_at = isset($row['update_on']) && $row['update_on'] != 0 ? convert_to_datetime($row['update_on']) : NULL;

                if (!is_array($check_record)) {
                    // Insert query for portal settings
                    if (in_array($key, $key_array_values)) {
                        $key_value_details = $this->__cleanse_key_value_portal_settings($key, $value);
                        if (!empty($key_value_details) && $key_value_details) {
                            // insert data with the unnormalized value from staging tables
                            foreach ($key_value_details as $detail) {
                                $portalRNAFields = array(
                                    'ffa_id'      => $ffa_id,
                                    'deleted'     => 0,
                                    'module'      => $row['module'],
                                    'type'      => $row['type'],
                                    '[key]'       => $detail['key'],
                                    'value'     => $detail['value'],
                                    'country'     => $this->country['country_name'],
                                    'sync_month'  => date('F-Y'),
                                    'ffa_module'=> $module,
                                    'update_on'  => $updated_at,
                                    'update_by'  => $row['update_by']
                                );

                                $portalSettingData[] = $portalRNAFields;
                            }
                        }
                    }else{
                        // insert data with the normalized value from staging tables
                        $portalRNAFields = array(
                            'ffa_id'      => $ffa_id,
                            'deleted'     => 0,
                            'module'      => $row['module'],
                            'type'      => $row['type'],
                            '[key]'       => $key,
                            'value'     => $value,
                            'country'     => $this->country['country_name'],
                            'sync_month'  => date('F-Y'),
                            'ffa_module'=> $module,
                            'update_on'  => $updated_at,
                            'update_by'  => $row['update_by']
                        );

                        $portalSettingData[] = $portalRNAFields;
                        // $strColumns = implode(', ', array_keys($portalRNAFields));
                        // $strValues =  " '" . implode("', '", array_values($portalRNAFields)) . "' ";
                        // $portSettingsInsertQuery = "INSERT INTO [$this->schemaName].[$this->analyticalTable] ({$strColumns}) VALUES ({$strValues});";
                        // $last_insert_id = $ffa_id;
                        // $result = $this->exec_query($portSettingsInsertQuery);
                        // if ($result) {
                        //     $countPortalSettings += 1;
                        // }
                    }
                } else {
                    // Update query for portal settings
                    $country = $this->country['country_name'];
                    if (in_array($key, $key_array_values)) {
                        $key_value_details = $this->__cleanse_key_value_portal_settings($key, $value);
                        if (!empty($key_value_details) && $key_value_details) {
                            // Update data with the unnormalized value from staging tables
                            foreach ($key_value_details as $detail) {

                                $portSettingsQuery = "
                                    UPDATE [$this->schemaName].[$this->analyticalTable]  
                                    SET
                                        [module] = '{$row['module']}',
                                        [type]   = '{$row['type']}',
                                        [key]  = '{$key}',
                                        [value]  = '{$value}',
                                        [update_on]  = '{$updated_at}',
                                        [update_by]  = '{$row['update_by']}'
                                    WHERE ffa_id = '$ffa_id' 
                                    AND country = '$country'; \n";
                                $res = $this->exec_query($portSettingsQuery);
                                if ($res) {
                                    $countPortalSettings += 1;
                                }
                            }
                        }
                        
                    }else{
                        // Update data with the normalized value from staging tables
                        
                        $portSettingsQuery = "
                            UPDATE [$this->schemaName].[$this->analyticalTable] 
                            SET
                                [module] = '{$row['module']}',
                                [type]   = '{$row['type']}',
                                [key]  = '{$key}',
                                [value]  = '{$value}',
                                [update_on]  = '{$updated_at}',
                                [update_by]  = '{$row['update_by']}'
                            WHERE [ffa_id] = '$ffa_id'
                            AND country = '$country';";
                        $res = $this->exec_query($portSettingsQuery);
                        if ($res) {
                            $countPortalSettings += 1;
                        }
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

    private function __cleanse_key_value_portal_settings($key, $value)
    {
        // keys and values for meeting types
        $details = [];
        if ($key == "meeting-types") {
            $temp_value = explode(';', $value);
            if (!empty($temp_value) && $temp_value) {
                foreach ($temp_value as $tkey => $val) {
                    $details[] = [
                        'key'   => $key.'-'.$tkey,
                        'value' => $val
                    ];
                }
            }
        }

        // keys and values for minimum execution time
        if ($key == "minimum-execution-time-limit-demo" || 
            $key == "minimum-execution-time-limit-meeting" ||
            $key == "minimum-execution-time-limit-farmer-visit"
            ) {
            $temp_value = explode('|', $value);
            if (!empty($temp_value) && $temp_value) {
                foreach ($temp_value as $tkey => $val) {
                    if ($tkey == 0) {
                        $details[] = [
                            'key'   => $key.'-input',
                            'value' => trim($val)
                        ];
                    }else{
                        $details[] = [
                            'key'   => $key.'-toggle',
                            'value' => trim($val)
                        ];
                    }
                }
            }
        }

        // keys and values for working hours
        if ($key == "working-hours-demo" || 
            $key == "working-hours-meeting" ||
            $key == "working-hours-retailer"
            ) {
            $temp_value = json_decode($value);
            if (!empty($temp_value) && $temp_value) {
                $details[] = [
                    'key'   => $key.'-start',
                    'value' => $temp_value->start
                ];

                $details[] = [
                    'key'   => $key.'-end',
                    'value' => $temp_value->end
                ];
            }
        }

        return $details;
    }

    // Get the portal settings module: demo, farmer meeting, retailer
    private function __get_ffa_module($key)
    {
        $key_array = explode('-', $key);

        $module = '';
        if (in_array('demo', $key_array)) {
            $module = 'demo';
        }

        if (in_array('meeting', $key_array)) {
            $module = 'meeting';
        }

        if (in_array('retailer', $key_array)) {
            $module = 'retailer';
        }

        return $module;
    }

    private function __checkPortalSettingRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id
        FROM [$this->schemaName].[$this->analyticalTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }
}
