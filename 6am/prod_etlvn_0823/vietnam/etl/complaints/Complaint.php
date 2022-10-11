<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Complaint extends DB
{

    protected $ffaTable = 'tbl_complaint';

    protected $analyticalTable = 'complaints';

    protected $stagingTable = '';

    protected $reportTable = 'complaints';

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


    public function getStaging()
    {
        $sql = "SELECT
            id,
            ffa_id,
            complainant_type,
            complaint_type,
            target_close_date,
            name,
            phone,
            resolver_group,
            call_type,
            activity_code,
            team,
            create_on,
            status,
            territory,
            region,
            zone
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
        	report_table = '$this->reportTable' ";

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $countComplaintAffectedRows = 0;
        $complaintsData = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $lastInserted = $row['ffa_id'];

                $complaintsRnaFields = array(
                    'ffa_id'      => $row['ffa_id'],
                    'deleted' => 0,
                    'country'     => $this->country['country_name'],
                    'name'        => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['name'], ENT_QUOTES)),
                    'phone'       => $row['phone'],
                    'complainant_type' => $row['complainant_type'],
                    'complaint_type'   => $row['complaint_type'],
                    // 'target_close_date'=> $row['target_close_date'],
                    'resolver_group'   => $row['resolver_group'],
                    'call_type'        => $row['call_type'],
                    'activity_code'    => $row['activity_code'],
                    'team'             => $row['team'],
                    'status'           => $row['status'],
                    'territory_id' => $row['territory'],
                    'zone_id' => $row['zone'],
                    'region_id' => $row['region'],
                );
                
                if (isset($row['create_on']) && !empty($row['create_on']) && $row['create_on'] != '' && !is_null($row['create_on'])) {
                    $complaintsRnaFields['create_on'] = convert_to_datetime($row['create_on']);
                } else {
                    unset($complaintsRnaFields['create_on']);
                }
                
                if (isset($row['update_on']) && !empty($row['update_on']) && !is_null($row['update_on']) && $row['update_on'] !== '' && $row['update_on'] != null) {
                    $complaintsRnaFields['update_on'] = convert_to_datetime($row['update_on']);
                } else {
                    unset($complaintsRnaFields['update_on']);
                }

                if (isset($row['target_close_date']) && !empty($row['target_close_date']) &&  $row['target_close_date'] != '' && !is_null($row['target_close_date'])) {
                    $complaintsRnaFields['target_close_date'] = $row['target_close_date'];
                } else {
                    unset($complaintsRnaFields['target_close_date']);
                }

                if (isset($row['closed_on']) && !empty($row['closed_on']) &&  $row['closed_on'] != '' && !is_null($row['closed_on'])) {
                    $complaintsRnaFields['close_on'] = $row['closed_on'];
                } else {
                    unset($complaintsRnaFields['closed_on']);
                }

                $complaintsData[] = $complaintsRnaFields;
            }

            foreach ($complaintsData as $data) {
                $check_record = $this->__checkComplaintsRecord($lastInserted);

                if (!is_array($check_record)) {
                    $strColumns = implode(', ', array_keys($data));
                    $strValues =  " '" . implode("', '", array_values($data)) . "' ";
                    $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues});";

                    $result = $this->exec_query($userInsertQuery);
                    if ($result) {
                        $countComplaintAffectedRows += 1;
                    }
                    
                } else {
                    $country = $this->country['country_name'];
                    $ffaId = $data['ffa_id'];
                    $name = preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($data['name'], ENT_QUOTES));
                    $target_close_date = (isset($data['target_close_date'])) ? 'target_close_date = '."'{$data['target_close_date']}'," : null;

                    if (isset($data['closed_on'])) {
                        $convert_closed_on = convert_to_datetime($data['closed_on']);
                        $close_on = 'close_on = '."'{$convert_closed_on}',";
                    } else {
                        $close_on = null;
                    }
                    
                    $userUpdateQuery = "
                            UPDATE [$this->schemaName].[$this->reportTable]
                            SET 
                                name = '{$name}',
                                phone = '{$data['phone']}',
                                complainant_type = '{$data['complainant_type']}',
                                complaint_type = '{$data['complaint_type']}',
                                resolver_group = '{$data['resolver_group']}',
                                call_type = '{$data['call_type']}',
                                activity_code = '{$data['activity_code']}',
                                team = '{$data['team']}',
                                status = '{$data['status']}',
                                {$target_close_date}
                                {$close_on}
                                deleted      = 0
                        WHERE [ffa_id] = '$ffaId'
                        AND country = '$country';";

                    $result =  $this->exec_query($userUpdateQuery);
                    if ($result) {
                        $countComplaintAffectedRows += 1;
                    }
                }
            }
            
            return [
                'num_rows'    => $countComplaintAffectedRows,
                'last_insert_id'  => $lastInserted
            ];

        } else {
            $message = "No complaints Records to sync";
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

    private function __checkComplaintsRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id
        FROM [$this->schemaName].[$this->reportTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
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
}
