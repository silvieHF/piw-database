CREATE TABLE fastas_todo (
	name TEXT,
	source_type TEXT,
	source_id TEXT,
	PRIMARY KEY(name, source_type, source_id)
);

CREATE INDEX fastas_todo_idx ON fastas_todo(name, source_type, source_id);

CREATE TABLE fastas_done (
	id BIGSERIAL PRIMARY KEY NOT NULL,
	name TEXT,
	source_type TEXT,
	source_id TEXT,
	fasta TEXT
);

CREATE INDEX fastas_done_idx ON fastas_done(name, source_type, source_id);
