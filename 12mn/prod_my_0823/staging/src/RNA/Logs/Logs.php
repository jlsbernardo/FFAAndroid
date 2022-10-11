<?php

require_once dirname(__DIR__).'/Contracts/LogsContract.php';

class Logs implements LogsContract
{
    /**
     * Log path to directory
     *
     * @var string
     */
    protected static $path = '';

    /**
     * Log Message
     *
     * @var string
     */
    protected static $message = '';

    /**
     * Error Log
     *
     * @param string $message
     * @param string $path
     * @return string
     */
    public static function error(string $message, string $path = '')
    {
        self::$message = $message;

        self::getPath($path, 'error');
        
        self::readWriteFile();

        return $message;
    }

    /**
     * Success Log
     *
     * @param string $message
     * @param string $path
     * @return string
     */
    public static function success(string $message, string $path = '')
    {
        self::$message = $message;
        
        self::getPath($path, 'success');

        self::readWriteFile();

        return $message;
    }

    /**
     * Get Log Path
     *
     * @param string $path
     * @param string $type
     * @return string
     */
    private static function getPath(string $path, string $type)
    {   
        $logFile =  date('Y-m-d').'-'.$type.'.log';
        if ($path == '') {
             $path = dirname(__DIR__, 3).'/logs';
        }

        if (!file_exists($path)) {
            mkdir($path, 0777, true);
        }

        static::$path = $path. '/' . $logFile;

        return self::$path;
    }

    /**
     * Read and Write  Log Files
     */
    private function readWriteFile()
    {
        $message = self::$message;
        $logMsg = "[" . date('Y-m-d H:i:s') . "]: $message";

        $fp = fopen(self::$path, 'a');

        fwrite($fp, $logMsg);
        fclose($fp);
        chmod(self::$path, 0644);
    }
}