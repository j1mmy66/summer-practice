-- Одна запись на (date, kind). kind ∈ {train, test, future}
CREATE TABLE IF NOT EXISTS public.predictions (
    date             date              NOT NULL,
    kind             text              NOT NULL,
    predicted_close  numeric(18,6)     NOT NULL,
    imported_at      timestamptz       NOT NULL DEFAULT now(),
    CONSTRAINT predictions_pk PRIMARY KEY (date, kind),
    CONSTRAINT predictions_kind_chk CHECK (lower(kind) IN ('train','test','future'))
);

CREATE INDEX IF NOT EXISTS idx_predictions_date ON public.predictions (date);
