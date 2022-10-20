<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

/**
 * This script is for boilerplate of reports scripts per country (ie. demo_reports_ph.php, demo_reports_vn.php etc.)
 * 
 * @return \Logs
 */


class Meeting extends DB
{

    protected $ffaTable = 'tbl_ffa_meeting';

    protected $stagingTable = '';

    protected $reportTable = 'farmer_meeting_reports';

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

        parent::__construct();
    }

    /**
     * Check if data is deleted
     *
     * @param  $ffa_id
     * @return void
     */
    private function __checkDeleted($ffa_id)
    {
        echo Logs::success("1AM ID Meeting __checkDeleted Process Start: " . date('Y-m-d H:i:s') . "\n");
        $sql = "SELECT ffa_id FROM tbl_deleted_activities WHERE module = '$this->reportTable' AND 'ffa_id' = $ffa_id limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            // return true;
            $row = $result->fetch_assoc();
            if (date('Y-m-d H:i') >= $row['deleted_at']) {
                return true;
            }
        }
        echo Logs::success("1AM ID Meeting __checkDeleted Process End: " . date('Y-m-d H:i:s') . "\n");
        return false;
    }

    /**
     * Get Data From Staging Table
     *
     * @return array | string
     */
    public function getStaging()
    {
        echo Logs::success("1AM ID Meeting getStaging Process Start: " . date('Y-m-d H:i:s') . "\n");
        $meetingSql = "SELECT
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
        date,
        team,
        host_name,
        host_phone,
        month,
        status,
        temp_execute,
        products,
        territory,
        zone,
        region,
        participant_list,
        supervisor,
        marked_by,
        marked_on,
        crop_id,
        lat,
        lng
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        order by
            create_on
        desc";

        $meetingInsertQuery = "";
        $result = $this->exec_query($meetingSql);
        $duplicate = [];
        $meetingRNAFields = [];

        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $temp_execute = json_decode($row['temp_execute'], true);

                if ($row['status'] == 'close') {
                    if (!is_null($row['closed_on']) && $row['closed_on'] != '') {
                        $duplicate[$row['id']] = [
                            'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                            'month' => convert_to_datetime( $row['closed_on'], 'Y-m-d')
                        ];
                    }
                }
                
                $meetingRNAFields[] = array(
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
                    'date'  => ($row['date']),
                    // 'month' => $row['month'],
                    'month' => date("M-Y",$row['create_on'] ),
                    'team'  => $row['team'],
                    'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                    'host_phone'    => $row['host_phone'],
                    'status' => $row['status'],
                    'month' => $row['month'],
                    'products' => $row['products'],
                    'territory_id'   => $row['territory'],
                    'zone_id'   => $row['zone'],
                    'region_id'   => $row['region'],
                    'country'   => $this->country['country_name'],
                    'client_execute_submit_date_time'   => $row['temp_execute'] ? $row['temp_execute'] : null,
                    'participant_list'  => $row['participant_list'],
                    'supervisor'    => $row['supervisor'],
                    'marked_by' => !is_null($row['marked_by']) ? $row['marked_by'] : 0,
                    'marked_on' => $row['marked_on'],
                    'crop_id'   => $row['crop_id'],
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
            $minFarmerParticipants = $this->getPortalSettingsKey('minimum-farmer-participants');
            
            $countMeetingAffectedRows = 0;
            foreach ($meetingRNAFields as $key => $meetingRNAField) {

                $isDuplicate = 0;
                $isOffBusinessHours = 0;
                $uniqHost = '';
                $uniqMonth = '';
                $isInvalidParticipantCount = 0;
                
                if ($meetingRNAField['host_phone'] != '' && !is_null($meetingRNAField['host_phone'])) {
                    $hostPhone = str_replace($phonePrefix, '', $meetingRNAField['host_phone']);
                    if( is_array($phoneLength) && count($phoneLength)>0 ){
                        if (strlen($hostPhone) < $phoneLength[0] || strlen($hostPhone) > $phoneLength[1]) {
                            $meetingRNAField['incorrect_data'] = 1;
                        }
                    }
                }

                if ($meetingRNAField['status'] != 'close') {
                    $meetingRNAField['incomplete_activity'] = 1;
                }
                
                if (!empty($meetingRNAField['plan_on']) && $meetingRNAField['plan_on'] != '0' && !is_null($meetingRNAField['plan_on'])) {
                    $meetingRNAField['plan_on'] = convert_to_datetime($meetingRNAField['plan_on']);
                } else {
                    unset($meetingRNAField['plan_on']);
                }
                
                if (isset($meetingRNAField['approved_on']) && !empty($meetingRNAField['approved_on']) && $meetingRNAField['approved_on'] != '0' && !is_null($meetingRNAField['approved_on'])) {
                    $meetingRNAField['approved_on'] = convert_to_datetime($meetingRNAField['approved_on']);
                } else {
                    unset($meetingRNAField['approved_on']);
                }

                if (isset($meetingRNAField['closed_on']) && !empty($meetingRNAField['closed_on']) && $meetingRNAField['closed_on'] != '0' && !is_null($meetingRNAField['closed_on'])) {
                    $meetingRNAField['closed_on'] = convert_to_datetime($meetingRNAField['closed_on']);
                } else {
                    unset($meetingRNAField['closed_on']);
                }

                if (isset($meetingRNAField['date']) && !empty($meetingRNAField['date']) && !is_null($meetingRNAField['date']) && $meetingRNAField['date'] != '1970-01-01 00:00:00') {
                    $meetingRNAField['date'] = $meetingRNAField['date'];
                } else {
                    unset($meetingRNAField['date']);
                }

                if (isset($meetingRNAField['client_execute_submit_date_time']) && !is_null($meetingRNAField['client_execute_submit_date_time'])) {
                    $meetingRNAField['client_execute_submit_date_time'] = convert_to_datetime($meetingRNAField['client_execute_submit_date_time'], 'Y-m-d H:i:s');

                    $clientExecute = date('H:i', strtotime($meetingRNAField['client_execute_submit_date_time']));
                    $isRange = ($offBusinessHours['start']!==null && $offBusinessHours['end']!==null && $clientExecute!==null) ? time_in_range($offBusinessHours['start'], $offBusinessHours['end'], $clientExecute) : false;
                
                    if (!$isRange) {
                        $isOffBusinessHours = 1;
                    }
                    
                    $meetingRNAField['off_business_hours'] = $isOffBusinessHours;
                    
                } else {
                    unset($meetingRNAField['client_execute_submit_date_time']);
                }

                if (isset($meetingRNAField['marked_on']) && !is_null($meetingRNAField['marked_on'])) {
                    $meetingRNAField['marked_on'] = convert_to_datetime($meetingRNAField['marked_on']);
                } else {
                    unset($meetingRNAField['marked_on']);
                }

                //check for duplicate data
                foreach ($diffCellUniq as $uniq) {
                    $uniqHost = $uniq['host_name'];
                    $uniqMonth = $uniq['month'];

                    if (isset($meetingRNAField['closed_on']) && !is_null($uniqMonth)) {
                        if (
                            $meetingRNAField['host_name'] == $uniqHost &&
                            date('Y-m-d', strtotime($meetingRNAField['closed_on'])) == $uniqMonth
                        ) {
                            $isDuplicate = 1;
                        }
                    }
                }

                if (isset($meetingRNAField['participant_list']) && !is_null($meetingRNAField['participant_list']) && $meetingRNAField['participant_list'] != '') {
                    $participantList = json_decode($meetingRNAField['participant_list'], TRUE);
                    if (is_array($participantList)) {
                        if (count($participantList) < $minFarmerParticipants['value']) {
                            $isInvalidParticipantCount = 1;
                        }
                    }
                }

                $lastInserted = $meetingRNAField['ffa_id'];
                $check_record = $this->__checkMeetingRecord($lastInserted);
                
                $meetingRNAField['duplicate_data'] = $isDuplicate;
                $meetingRNAField['invalid_participant_count'] = $isInvalidParticipantCount;
                
                if (!is_array($check_record)) {
                    $removeSpecificColumns = array_diff_key($meetingRNAField, array_flip(["participant_list"]));
                    
                    $strColumns = implode(', ', array_keys($removeSpecificColumns));
                    $strValues =  " '" . implode("', '", array_values($removeSpecificColumns)) . "' ";
                    $meetingInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues});";
    
                    $result = $this->exec_query($meetingInsertQuery);
                    if ($result) {
                        $countMeetingAffectedRows += 1;
                    }

                } else {
                    $country = $this->country['country_name'];
                    $plan_on = (isset($meetingRNAField['plan_on'])) ? 'plan_on = '."'{$meetingRNAField['plan_on']}'," : null;
                    $approved_on = (isset($meetingRNAField['approved_on'])) ? 'approved_on ='."'{$meetingRNAField['approved_on']}'," : null;
                    $closed_on = (isset($meetingRNAField['closed_on'])) ? 'closed_on ='."'{$meetingRNAField['closed_on']}'," : null;
                    $date = isset($meetingRNAField['date']) ? 'date ='."'{$meetingRNAField['date']}'," : null;
                    $client_execute_submit_date_time = isset($meetingRNAField['client_execute_submit_date_time']) ? 'client_execute_submit_date_time ='."'{$meetingRNAField['client_execute_submit_date_time']}'," : null;
                    $markedBy = !is_null($meetingRNAField['marked_by']) ? $meetingRNAField['marked_by'] : 0;
                    $markedOn = isset($meetingRNAField['marked_on']) ? 'marked_on =' . "'{$meetingRNAField['marked_on']}'," : null;
                    
                    $demoUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->reportTable]
                        SET 
                            update_on= '{$meetingRNAField['update_on']}',
                            update_by = '{$meetingRNAField['update_by']}',
                            deleted = '{$meetingRNAField['deleted']}',
                            {$plan_on}
                            plan_by   = '{$meetingRNAField['plan_by']}',
                            {$approved_on}
                            approved_by = '{$meetingRNAField['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$meetingRNAField['closed_by']}',
                            {$date}
                            team      = '{$meetingRNAField['team']}',
                            host_name      = '{$meetingRNAField['host_name']}',
                            host_phone      = '{$meetingRNAField['host_phone']}',
                            month      = '{$meetingRNAField['month']}',
                            status = '{$meetingRNAField['status']}',
                            products      = '{$meetingRNAField['products']}',
                            territory_id      = '{$meetingRNAField['territory_id']}',
                            zone_id      = '{$meetingRNAField['zone_id']}',
                            country      = '{$this->country['country_name']}',
                            {$client_execute_submit_date_time}
                            supervisor = '{$meetingRNAField['supervisor']}',
                            marked_by = '{$markedBy}',
                            {$markedOn}
                            region_id      = '{$meetingRNAField['region_id']}',
                            crop_id     = '{$meetingRNAField['crop_id']}',
                            lat      = '{$meetingRNAField['lat']}',
                            lng      = '{$meetingRNAField['lng']}'
                        WHERE ffa_id = '$lastInserted'
                        AND country = '$country';";

                    $result =  $this->exec_query($demoUpdateQuery);
                    if ($result) {
                        $countMeetingAffectedRows += sqlsrv_rows_affected($result);
                    }
                }
            }

            return [
                'num_rows'    => $countMeetingAffectedRows,
                'last_insert_id'  => $lastInserted
            ];

        } else {
            $message = "No meeting Reports Records to sync";
            return $message;
        }
        echo Logs::success("1AM ID Meeting getStaging Process End: " . date('Y-m-d H:i:s') . "\n");
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
        echo Logs::success("1AM ID Meeting updateRNAEtlSync Process Start: " . date('Y-m-d H:i:s') . "\n");
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
        echo Logs::success("1AM ID Meeting updateRNAEtlSync Process End: " . date('Y-m-d H:i:s') . "\n");
    }

    // Check the user record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync($lastInserted)
    {
        echo Logs::success("1AM ID Meeting __checkRecordFFASync Process Start: " . date('Y-m-d H:i:s') . "\n");
        $where = (!is_null($lastInserted) && $lastInserted != '') ? 'AND last_insert_id = ' . $lastInserted : '';
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' $where ORDER BY id DESC LIMIT 1";
        
        $results = $this->exec_query($sql);
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        echo Logs::success("1AM ID Meeting __checkRecordFFASync Process End: " . date('Y-m-d H:i:s') . "\n");
        return false;
    }

    private function __checkMeetingRecord($ffa_id)
    {
        echo Logs::success("1AM ID Meeting __checkMeetingRecord Process Start: " . date('Y-m-d H:i:s') . "\n");
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 [id]
        FROM 
            [$this->schemaName].[$this->reportTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }
        echo Logs::success("1AM ID Meeting __checkMeetingRecord Process End: " . date('Y-m-d H:i:s') . "\n");
        return false;
    }

    private function getPortalSettingsKey($key)
    {
        echo Logs::success("1AM ID Meeting getPortalSettingsKey Process Start: " . date('Y-m-d H:i:s') . "\n");
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
        echo Logs::success("1AM ID Meeting getPortalSettingsKey Process End: " . date('Y-m-d H:i:s') . "\n");
    }

    private function getOffBusinessHours()
    {
        echo Logs::success("1AM ID Meeting getOffBusinessHours Process Start: " . date('Y-m-d H:i:s') . "\n");
        $workingHoursSQL = "SELECT
           [id],
           [module]
            [key],
            [value]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE [module] = 'working-hours'";

        $workingHours = $this->exec_query($workingHoursSQL);

        if (sqlsrv_num_rows($workingHours) > 0) {

            while ($row = sqlsrv_fetch_array($workingHours)) {
                $value = json_decode($row['value']);
                
                $res['start'] = $value->start;
                $res['end'] = $value->end;
                return $res;
            }
        }
        echo Logs::success("1AM ID Meeting getOffBusinessHours Process End: " . date('Y-m-d H:i:s') . "\n");
    }

}
