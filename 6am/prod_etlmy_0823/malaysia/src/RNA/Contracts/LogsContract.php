<?php

interface LogsContract
{
    /**
     * Error Log
     *
     * @param string $message
     * @param string $path
     * @return string
     */
    public static function error(string $message, string $path = '');

    /**
     * Success Log
     *
     * @param string $message
     * @param string $path
     * @return string
     */
    public static function success(string $message, string $path= '');
}