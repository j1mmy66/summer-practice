-- Будет выполнен при первом старте контейнера (внутри БД 'stocks')
-- Одна таблица для всех компаний и дат

CREATE TABLE IF NOT EXISTS public.quotes (
    company    text        NOT NULL,
    date       date        NOT NULL,
    open       numeric(18,6) NOT NULL,
    high       numeric(18,6) NOT NULL,
    low        numeric(18,6) NOT NULL,
    close      numeric(18,6) NOT NULL,
    adj_close  numeric(18,6) NOT NULL,
    volume     bigint      NOT NULL,
    CONSTRAINT quotes_pk PRIMARY KEY (company, date)
);

CREATE INDEX IF NOT EXISTS idx_quotes_date ON public.quotes (date);
CREATE INDEX IF NOT EXISTS idx_quotes_company_date ON public.quotes (company, date DESC);
