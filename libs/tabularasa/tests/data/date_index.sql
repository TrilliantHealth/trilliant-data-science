CREATE UNIQUE INDEX idx_dates_date1_datetime2 ON dates(date1, datetime2);
CREATE UNIQUE INDEX idx_dates_datetime2_datetime1 ON dates(datetime2, datetime1);
