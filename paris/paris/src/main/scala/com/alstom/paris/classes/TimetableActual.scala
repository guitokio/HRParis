package com.alstom.paris.classes

import java.sql.Timestamp

/**
 *  Base case class used for the preliminary parsing of the actual timetable files
 */
case class TimetableActual (
    stamp: Timestamp,
    line: String,
    stop: String,
    direction: String,
    arrival_time: String,
    delay: String,
    message: String,
    next: Boolean,
    all: String
)
