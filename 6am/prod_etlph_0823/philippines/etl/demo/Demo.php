
<?php

require_once (dirname(__FILE__, 3). '/src/RNA/Database/DB.php');

class Demo extends DB
{
    protected $ffaTable = 'tbl_ffa_demo';

    protected $stagingTable = '';

    protected $reportTable = 'demo_reports';

    protected $ffaGps = 'tbl_ffa_images';

    protected $country = '';

    protected $connection;

    protected $ffaSyncTable = 'tbl_rna_etl_sync';
    
    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;

        //call DB contructor
        parent::__construct();
    }

    private function getLastInsertedId()
    {
        $sql = "SELECT ffa_id FROM staging_ph WHERE report_table = 'demo_reports' ORDER BY id desc limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            return $result;
        }
    }

    private function getExistingInStaging($ffa_id)
    {
        $sql = "SELECT ffa_id FROM staging_ph WHERE report_table = 'demo_reports' AND 'ffa_id' = $ffa_id limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            return $result->fetch_assoc();
        }

        return false;
    }

    private function __checkDeleted($ffa_id)
    {
        $sql = "SELECT ffa_id FROM tbl_deleted_activities WHERE module = '$this->reportTable' AND 'ffa_id' = $ffa_id limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            // return true;
            $row = $result->fetch_assoc();
            if (date('Y-m-d H:i') >= $row['deleted_at']) {
                return true;
            }
        }

        return false;
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

    // Check the user record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync($lastInserted)
    {
        $where = (!is_null($lastInserted) && $lastInserted != '') ? 'AND last_insert_id = ' . $lastInserted : '';
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' $where ORDER BY id DESC LIMIT 1";
        
        $results = $this->exec_query($sql);
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }

    /**
     * Get Data From Staging Table
     *
     * @return array | string
     */
    public function getStaging()
    {
        $demoSql = "SELECT
        DISTINCT 
        id,
        ffa_id,
        deleted,
        create_on,
        created_by,
        update_on,
        update_by,
        plan_on,
        plan_by,
        approved_on,
        approved_by,
        closed_on,
        closed_by,
        execute_on,
        execute_by,
        date,
        team,
        host_name,
        host_phone,
        month,
        status,
        temp_followup,
        temp_execute,
        products,
        territory,
        zone,
        region,
        followup,
        supervisore,
        supervisorf,
        marked_by_e,
        marked_by_f,
        marked_on_e,
        marked_on_f,
        crop_id,
        lat,
        lng
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        order by
            id
        desc";

        $demoInsertQuery = "";
        $result = $this->exec_query($demoSql);

        $duplicate = [];
        $demoRNAFields = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $products = json_decode($row['products'], true);
                
                $temp_execute = json_decode($row['temp_execute'], true);
                $temp_followup = json_decode($row['temp_followup'], true);

                if ($row['status'] == 'close') {
                    if (!is_null($row['closed_on']) && $row['closed_on'] != '') {
                        $duplicate[$row['id']] = [
                            'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                            'products'  => (!empty($products[0]['caption'])) ? $products[0]['caption'] : null,
                            'product_id'    => isset($products[0]) ? $products[0]['id'] : 0,
                            'month' => convert_to_datetime( $row['closed_on'], 'Y-m-d')
                        ];
                    }
                }

                $demoRNAFields[] = array(
                    'ffa_id' => $row['ffa_id'],
                    'deleted' => $row['deleted'],
                    'create_on' => convert_to_datetime($row['create_on']),
                    'created_by' => $row['created_by'],
                    'update_on' => ($row['update_on'] != '0' && $row['update_on'] != null) ? convert_to_datetime($row['update_on']) : '1970-01-01 00:00:00',
                    'update_by' => $row['update_by'],
                    'plan_on'   => $row['plan_on'],
                    'plan_by'   => $row['plan_by'],
                    'approved_on'  => $row['approved_on'],
                    'approved_by'   => $row['approved_by'],
                    'closed_on'  => $row['closed_on'],
                    'closed_by'   => $row['closed_by'],
                    'execute_on'  => $row['execute_on'],
                    'execute_by'   => $row['execute_by'],
                    'date'  => $row['date'],
                    'followup'  => ($row['followup']) ? $row['followup'] : null,
                    'team'  => $row['team'],
                    'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                    'host_phone'    => $row['host_phone'],
                    // 'month' => $row['month'],
                    'month' => date("M-Y",$row['create_on'] ),
                    'status' => $row['status'],
                    'product_id' => isset($products[0]) ? $products[0]['id'] : 0,
                    'product_detail' => (!empty($products[0]['detail'])) ? $products[0]['detail'] : '',
                    'product_caption' => (!empty($products[0]['caption'])) ? $products[0]['caption'] : '',
                    'territory_id'   => $row['territory'],
                    'zone_id'   => $row['zone'],
                    'country'   => $this->country['country_name'],
                    'region_id'   => $row['region'],
                    'supervisore' => $row['supervisore'],
                    'supervisorf' => $row['supervisorf'],
                    'marked_by_e' => !is_null($row['marked_by_e']) ? $row['marked_by_e'] : 0,
                    'marked_by_f' => !is_null(($row['marked_by_f'])) ? $row['marked_by_f'] : 0,
                    'marked_on_e' => $row['marked_on_e'],
                    'marked_on_f' => $row['marked_on_f'],
                    'client_execute_submit_date_time'   => $row['temp_execute'] ? $row['temp_execute'] : null,
                    'client_followup_submit_date_time'   => $row['temp_followup'] ? $row['temp_followup'] : null,
                    'crop_id' => $row['crop_id'],
                    'lat'     => $row['lat'],
                    'lng'     => $row['lng']
                );
            }
            
            $arr_uniq = array_unique($duplicate, SORT_REGULAR);

            // get the duplicated data
            $diffCellUniq = array_diff_key($duplicate, $arr_uniq);
            
            $incorrectData = $this->getPortalSettingsKey('phone-length-restriction');
            $phoneLength = isset($incorrectData['value']) ? explode('|', $incorrectData['value']):[];
            $phonePrefix = isset($this->getPortalSettingsKey('phone-prefix')['value'])? $this->getPortalSettingsKey('phone-prefix')['value']:'';
            
            $offBusinessHours = $this->getOffBusinessHours();
            $countDemoAffectedRows = 0;
            foreach ($demoRNAFields as $key => $demoRNAField) {
                
                $isDuplicate = 0;
                $isOffBusinessHours = 0;
                $uniqHost = '';
                $uniqMonth = '';
                
                if ($demoRNAField['host_phone'] != '' && !is_null($demoRNAField['host_phone'])) {
                    $hostPhone = str_replace($phonePrefix, '', $demoRNAField['host_phone']);
                    if( is_array($phoneLength) && count($phoneLength)>0 ){
                        if (strlen($hostPhone) < $phoneLength[0] || strlen($hostPhone) > $phoneLength[1]) {
                            $demoRNAField['incorrect_data'] = 1;
                        }
                    }
                   
                }

                if ($demoRNAField['status'] != 'close') {
                    $demoRNAField['incomplete_activity'] = 1;
                }
                
                if (!empty($demoRNAField['plan_on']) && $demoRNAField['plan_on'] != '0' && !is_null($demoRNAField['plan_on'])) {
                    $demoRNAField['plan_on'] = convert_to_datetime($demoRNAField['plan_on']);
                } else {
                    unset($demoRNAField['plan_on']);
                }
                
                if (isset($demoRNAField['approved_on']) && !empty($demoRNAField['approved_on']) && $demoRNAField['approved_on'] != '0' && !is_null($demoRNAField['approved_on'])) {
                    $demoRNAField['approved_on'] = convert_to_datetime($demoRNAField['approved_on']);
                } else {
                    unset($demoRNAField['approved_on']);
                }

                if (isset($demoRNAField['closed_on']) && !empty($demoRNAField['closed_on']) && $demoRNAField['closed_on'] != '0' && !is_null($demoRNAField['closed_on'])) {
                    $demoRNAField['closed_on'] = convert_to_datetime($demoRNAField['closed_on']);
                } else {
                    unset($demoRNAField['closed_on']);
                }

                if (isset($demoRNAField['execute_on']) && !empty($demoRNAField['execute_on']) && $demoRNAField['execute_on'] != '0' && !is_null($demoRNAField['execute_on'])) {
                    $demoRNAField['execute_on'] = convert_to_datetime($demoRNAField['execute_on']);
                } else {
                    unset($demoRNAField['execute_on']);
                }

                if (isset($demoRNAField['followup']) && !empty($demoRNAField['followup']) && !is_null($demoRNAField['followup']) && $demoRNAField['followup'] != '1970-01-01 00:00:00') {
                    $demoRNAField['followup'] = $demoRNAField['followup'];
                } else {
                    unset($demoRNAField['followup']);
                }

                if (isset($demoRNAField['date']) && !empty($demoRNAField['date']) && !is_null($demoRNAField['date']) && $demoRNAField['date'] != '1970-01-01 00:00:00') {
                    $demoRNAField['date'] = $demoRNAField['date'];
                } else {
                    unset($demoRNAField['date']);
                }

                if (isset($demoRNAField['client_execute_submit_date_time']) && !is_null($demoRNAField['client_execute_submit_date_time'])) {
                    $demoRNAField['client_execute_submit_date_time'] = convert_to_datetime($demoRNAField['client_execute_submit_date_time'], 'Y-m-d H:i:s');
                } else {
                    unset($demoRNAField['client_execute_submit_date_time']);
                }

                if (isset($demoRNAField['marked_on_e']) && !is_null($demoRNAField['marked_on_e'])) {
                    $demoRNAField['marked_on_e'] = convert_to_datetime($demoRNAField['marked_on_e']);
                } else {
                    unset($demoRNAField['marked_on_e']);
                }

                if (isset($demoRNAField['marked_on_f']) && !is_null($demoRNAField['marked_on_f'])) {
                    $demoRNAField['marked_on_f'] = convert_to_datetime($demoRNAField['marked_on_f']);
                } else {
                    unset($demoRNAField['marked_on_f']);
                }

                if (isset($demoRNAField['client_followup_submit_date_time']) && !is_null($demoRNAField['client_followup_submit_date_time'])) {
                    $demoRNAField['client_followup_submit_date_time'] = convert_to_datetime($demoRNAField['client_followup_submit_date_time'], 'Y-m-d H:i:s');

                    $clientFollowUp = date('H:i', strtotime($demoRNAField['client_followup_submit_date_time']));
                    $isRange = ($offBusinessHours['start']!==null && $offBusinessHours['end']!==null && $clientFollowUp!==null) ? time_in_range($offBusinessHours['start'], $offBusinessHours['end'], $clientFollowUp) : false;

                
                    if (!$isRange) {
                        $isOffBusinessHours = 1;
                    }
                    
                    $demoRNAField['off_business_hours'] = $isOffBusinessHours;
                    
                } else {
                    unset($demoRNAField['client_followup_submit_date_time']);
                }

                foreach ($diffCellUniq as $uniq) {
                    $uniqHost = $uniq['host_name'];
                    $uniqProductId = $uniq['product_id'];
                    $uniqMonth = $uniq['month'];

                    if (isset($demoRNAField['closed_on']) && !is_null($uniqMonth)) {
                        if (
                            $demoRNAField['host_name'] == $uniqHost &&
                            $demoRNAField['product_id'] == $uniqProductId &&
                            date('Y-m-d', strtotime($demoRNAField['closed_on'])) == $uniqMonth
                        ) {
                            $isDuplicate = 1;
                        }
                    }
                }
                
                $demoRNAField['duplicate_data'] = $isDuplicate;
                $demoRNAField['invalid_participant_count'] = 0;

                $lastInserted = $demoRNAField['ffa_id'];
                $check_record = $this->__checkDemoRecord($lastInserted);

                if (!is_array($check_record)) {
                    $strColumns = implode(', ', array_keys($demoRNAField));
                    $strValues =  " '" . implode("', '", array_values($demoRNAField)) . "' ";
                    $demoInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues}); \n";

                    $result =  $this->exec_query($demoInsertQuery);
                    if ($result) {
                        $countDemoAffectedRows += 1;
                    }

                } else {
                    $country = $this->country['country_name'];
                    $plan_on = (isset($demoRNAField['plan_on'])) ? 'plan_on = '."'{$demoRNAField['plan_on']}'," : null;
                    $approved_on = (isset($demoRNAField['approved_on'])) ? 'approved_on ='."'{$demoRNAField['approved_on']}'," : null;
                    $closed_on = (isset($demoRNAField['closed_on'])) ? 'closed_on ='."'{$demoRNAField['closed_on']}'," : null;
                    $date = isset($demoRNAField['date']) ? 'date ='."'{$demoRNAField['date']}'," : null;
                    $client_execute_submit_date_time = isset($demoRNAField['client_execute_submit_date_time']) ? 'client_execute_submit_date_time ='."'{$demoRNAField['client_execute_submit_date_time']}'," : null;
                    $client_followup_submit_date_time = isset( $demoRNAField['client_followup_submit_date_time']) ?  'client_followup_submit_date_time ='."'{$demoRNAField['client_followup_submit_date_time']}', " : null;

                    $markedByE = !is_null($demoRNAField['marked_by_e']) ? $demoRNAField['marked_by_e'] : 0;
                    $markedByF = !is_null($demoRNAField['marked_by_f']) ? $demoRNAField['marked_by_f'] : 0;
                    $markedOnE = isset($demoRNAField['marked_on_e']) ? 'marked_on_e =' . "'{$demoRNAField['marked_on_e']}'," : null;
                    $markedOnF = isset($demoRNAField['marked_on_f']) ? 'marked_on_f =' . "'{$demoRNAField['marked_on_f']}'," : null;
                    $offBusinessHour = isset( $demoRNAField['off_business_hours']) ?  'off_business_hours ='."'{$demoRNAField['off_business_hours']}', " : null;

                    $demoUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->reportTable]
                        SET 
                            update_on= '{$demoRNAField['update_on']}',
                            update_by = '{$demoRNAField['update_by']}',
                            deleted = '{$demoRNAField['deleted']}',
                            {$plan_on}
                            plan_by   = '{$demoRNAField['plan_by']}',
                            {$approved_on}
                            approved_by = '{$demoRNAField['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$demoRNAField['closed_by']}',
                            {$date}
                            team      = '{$demoRNAField['team']}',
                            host_name      = '{$demoRNAField['host_name']}',
                            host_phone      = '{$demoRNAField['host_phone']}',
                            month      = '{$demoRNAField['month']}',
                            status = '{$demoRNAField['status']}',
                            product_id      = '{$demoRNAField['product_id']}',
                            product_detail      = '{$demoRNAField['product_detail']}',
                            product_caption      = '{$demoRNAField['product_caption']}',
                            territory_id      = '{$demoRNAField['territory_id']}',
                            zone_id      = '{$demoRNAField['zone_id']}',
                            country      = '{$this->country['country_name']}',
                            supervisore = '{$demoRNAField['supervisore']}',
                            supervisorf = '{$demoRNAField['supervisorf']}',
                            marked_by_e = '{$markedByE}',
                            marked_by_f = '{$markedByF}',
                            {$markedOnE}
                            {$markedOnF}
                            {$client_execute_submit_date_time}
                            {$client_followup_submit_date_time}
                            {$offBusinessHour}
                            region_id      = '{$demoRNAField['region_id']}',
                            crop_id        = '{$demoRNAField['crop_id']}',
                            lat      = '{$demoRNAField['lat']}',
                            lng      = '{$demoRNAField['lng']}'
                        WHERE ffa_id = '$lastInserted'
                        AND country = '$country';";

                    $result =  $this->exec_query($demoUpdateQuery);
                    if ($result) {
                         $countDemoAffectedRows += sqlsrv_rows_affected($result);
                    }
                }
            }
            
            return [
                'num_rows'    => $countDemoAffectedRows,
                'last_insert_id'  => $lastInserted
            ];
            
        } else {
            $message = "No Demo Reports Records to sync";
            return $message;
        }
    }

    private function __checkDemoRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 [id]
        FROM 
            [$this->schemaName].[$this->reportTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }

    private function getPortalSettingsKey($key)
    {
        $sql = "SELECT TOP 1
            [id],
            [key],
            [value]
        FROM    
            [$this->schemaName].[$this->stagingTable]
        WHERE [key] = '$key'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            $row = sqlsrv_fetch_array($results);
            return $row;
        }
    }
                  
    private function getOffBusinessHours()
    {
        $workingHoursSQL = "SELECT
           [id],
           [module],
            [key],
            [value]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE [module] = 'working-hours' AND [key] = 'working-hours-demo'";

        $workingHours = $this->exec_query($workingHoursSQL);

        if (sqlsrv_num_rows($workingHours) > 0) {

            while ($row = sqlsrv_fetch_array($workingHours)) {
                $value = json_decode($row['value']);
                
                $res['start'] = $value->start;
                $res['end'] = $value->end;
                return $res;
            }
        }
    }

}