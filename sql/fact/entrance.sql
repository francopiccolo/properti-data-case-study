DROP TABLE IF EXISTS fact_entrance;

CREATE TABLE fact_entrance AS 

SELECT 
        EGID            AS	federal_building_identifier,
        EDID            AS	federal_entrance_identifier,
        EGAID           AS	federal_building_address_identifier,
        DEINR           AS	build_entrance_number,
        ESID            AS	federal_street_identifier,
        STRNAME         AS	street_name,
        STRNAMK         AS	abbreviated_street_name,
        STRINDX         AS	indexed_street_name,
        STRSP           AS	language_of_the_street_code,
        STROFFIZIEL     AS	official_name,
        DPLZ4           AS	postcode,
        DPLZZ           AS	complementary_to_npa,
        DPLZNAME        AS	place_name,
        DKODE           AS	e_coordinate_of_entrance,
        DKODN           AS	n_coordinate_of_entrance,
        DOFFADR         AS	official_address_code,
        DEXPDAT         AS	date_of_export
FROM    entrance;