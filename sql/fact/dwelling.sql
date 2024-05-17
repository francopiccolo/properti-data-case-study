DROP TABLE IF EXISTS fact_dwelling;

CREATE TABLE fact_dwelling AS 

SELECT
        CAST(EGID AS INTEGER)   AS federal_building_identifier,
        CAST(EWID AS INTEGER)   AS federal_dwelling_identifier,
        CAST(EDID AS INTEGER)   AS federal_entrance_identifier,
        WHGNR                   AS dwelling_administrative_number,
        WEINR                   AS dwelling_physical_number,
        CAST(WSTWK AS INTEGER)  AS floor_code,
        WBEZ                    AS floor_situation,
        CAST(WMEHRG AS INTEGER) AS several_floors_house_code,
        CAST(WBAUJ AS INTEGER)  AS dwelling_construction_year,
        CAST(WABBJ AS INTEGER)  AS dwelling_demolition_year,
        CAST(WSTAT AS INTEGER)  AS dwelling_state_code,
        CAST(WAREA AS INTEGER)  AS dwelling_surface_area,
        CAST(WAZIM AS INTEGER)  AS rooms_number,
        CAST(WKCHE AS INTEGER)  AS kitchen_code,
        WEXPDAT                 AS export_date
FROM    dwelling;