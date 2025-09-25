CREATE UNIQUE INDEX idx_strings_lowercase ON strings(lowercase);
CREATE INDEX idx_strings_lowercase_enum ON strings(lowercase, enum);
