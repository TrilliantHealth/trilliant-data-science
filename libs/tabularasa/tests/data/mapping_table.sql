CREATE TABLE mappings(
    pk INT16 NOT NULL,
    string_to_int_mapping JSON,
    int_to_string_mapping JSON
);

CREATE TABLE nested_mappings(
    string_to_int_array JSON NOT NULL,
    date_to_datetime_array JSON
);
