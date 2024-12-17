  CREATE TABLE public.income (
                series_reference character varying(256) ENCODE lzo,
                period real ENCODE raw,
                data_value character varying(256) ENCODE lzo,
                suppressed boolean ENCODE raw,
                status character varying(256) ENCODE lzo,
                units character varying(256) ENCODE lzo,
                magntude integer ENCODE az64,
                subject character varying(256) ENCODE lzo,
                group character varying(256) ENCODE lzo,
                series_title_1 character varying(256) ENCODE lzo
            )
            DISTSTYLE AUTO;