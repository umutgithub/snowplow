# Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
#
# Author(s): Yali Sassoon
# Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
# License: Apache License Version 2.0

-- Sessions basic table contains a line per individual session
-- The standard model identifies sessions using only first party cookies and session domain indexes

DROP TABLE IF EXISTS snowplow_intermediary.sessions_basic;
CREATE TABLE snowplow_intermediary.sessions_basic 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx)  -- optimized to join on other session_intermediary.session_X tables
	AS (
      SELECT
        domain_userid,
        domain_sessionidx,
        domain_userid || '-' || domain_sessionidx AS session_id,
        MIN(collector_tstamp) AS session_start_ts,
        MAX(collector_tstamp) AS session_end_ts,
        COUNT(*) AS number_of_events,
        COUNT(DISTINCT page_urlpath) AS distinct_pages_viewed
      FROM
        atomic.events
      GROUP BY 1,2,3
	);


-- Now create a table that assigns a geography to session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_geo;
CREATE TABLE snowplow_intermediary.sessions_geo 
	DISTKEY (domain_userid)
	SORTKEY (domain_userid, domain_sessionidx)
	AS (                      -- 3. Join with reference_data.country_codes
      SELECT
        v.domain_userid,
        v.domain_sessionidx,
        g.name AS geo_country,
        v.geo_country AS geo_country_code_2_characters,
        g.three_letter_iso_code AS geo_country_code_3_characters,
        v.geo_region,
        v.geo_city,
        v.geo_zipcode,
        v.geo_latitude,
        v.geo_longitude
      FROM (                   -- 2. Dedupe records (just in case there are two events with the same dvce_tstamp for a particular session)
        SELECT
          domain_userid,
          domain_sessionidx,
          geo_country, 
          geo_region,
          geo_city,
          geo_zipcode,
          geo_latitude,
          geo_longitude
        FROM (                 -- 1. Take first value for geography from each session
          SELECT
            domain_userid,
            domain_sessionidx,
            FIRST_VALUE(geo_country) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country,
            FIRST_VALUE(geo_region) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_region,
            FIRST_VALUE(geo_city) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_city,
            FIRST_VALUE(geo_zipcode) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_zipcode,
            FIRST_VALUE(geo_latitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_latitude,
            FIRST_VALUE(geo_longitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_longitude
          FROM atomic.events
          ) AS a
        GROUP BY 1,2,3,4,5,6,7,8
        ) AS v
        LEFT JOIN reference_data.country_codes AS g
        ON v.geo_country = g.two_letter_iso_code
	);

-- Now create a table that assigns a landing page to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_landing_page;
CREATE TABLE snowplow_intermediary.sessions_landing_page 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx)  -- optimized to join on other session_intermediary.session_X tables
	AS (
      SELECT
        domain_userid,
        domain_sessionidx,
        page_urlhost, 
        page_urlpath 
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          FIRST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
          FIRST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
        FROM atomic.events
        ) AS a
      GROUP BY 1,2,3,4
	);

-- Now create a table that assigns an exist page to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_exit_page;
CREATE TABLE snowplow_intermediary.sessions_exit_page 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx)  -- optimized to join on other session_intermediary.session_X tables
	AS (
      SELECT
        domain_userid,
        domain_sessionidx,
        page_urlhost, 
        page_urlpath 
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          LAST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
          LAST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
        FROM atomic.events

        ) AS a
      GROUP BY 1,2,3,4
	);

-- Now create a table that assigns campaign / referer data to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_source;
CREATE TABLE snowplow_intermediary.sessions_source 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx)  -- optimized to join on other session_intermediary.session_X tables
	AS (
      SELECT *
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          mkt_source,
          mkt_medium,
          mkt_campaign,
          mkt_term,
          refr_source,
          refr_medium,
          refr_term,
          refr_urlhost,
          refr_urlpath,
          dvce_tstamp,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx 
            ORDER BY dvce_tstamp, mkt_source, mkt_medium, mkt_campaign, mkt_term, refr_source, refr_medium, refr_term, refr_urlhost, refr_urlpath) AS "rank"
        FROM
          atomic.events
        WHERE
          refr_medium != 'internal' -- Not an internal referer
          AND (
            NOT(refr_medium IS NULL OR refr_medium = '') OR
            NOT ((mkt_campaign IS NULL AND mkt_content IS NULL AND mkt_medium IS NULL AND mkt_source IS NULL AND mkt_term IS NULL)
                    OR (mkt_campaign = '' AND mkt_content = '' AND mkt_medium = '' AND mkt_source = '' AND mkt_term = '')
            )
          )
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) AS t
      WHERE "rank" = 1 -- Only pull the first referer for each visit
	);

-- Now create a table that technology info per session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_technology;
CREATE TABLE snowplow_intermediary.sessions_technology 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx)  -- optimized to join on other session_intermediary.session_X tables
	AS (
       SELECT
        domain_userid,
        domain_sessionidx,
        br_name,
        br_family,
        br_version,
        br_type,
        br_renderengine,
        br_lang,
        br_features_director,
        br_features_flash,
        br_features_gears,
        br_features_java,
        br_features_pdf,
        br_features_quicktime,
        br_features_realplayer,
        br_features_silverlight,
        br_features_windowsmedia,
        br_cookies,
        os_name,
        os_family,
        os_manufacturer,
        os_timezone,
        dvce_type,
        dvce_ismobile,
        dvce_screenwidth,
        dvce_screenheight
      FROM (
        SELECT
          domain_userid,
          domain_sessionidx,
          br_name,
          br_family,
          br_version,
          br_type,
          br_renderengine,
          br_lang,
          br_features_director,
          br_features_flash,
          br_features_gears,
          br_features_java,
          br_features_pdf,
          br_features_quicktime,
          br_features_realplayer,
          br_features_silverlight,
          br_features_windowsmedia,
          br_cookies,
          os_name,
          os_family,
          os_manufacturer,
          os_timezone,
          dvce_type,
          dvce_ismobile,
          dvce_screenwidth,
          dvce_screenheight,
          RANK() OVER (PARTITION BY domain_userid, domain_sessionidx 
            ORDER BY dvce_tstamp, br_name, br_family, br_version, br_type, br_renderengine, br_lang, br_features_director, br_features_flash, 
            br_features_gears, br_features_java, br_features_pdf, br_features_quicktime, br_features_realplayer, br_features_silverlight,
            br_features_windowsmedia, br_cookies, os_name, os_family, os_manufacturer, os_timezone, dvce_type, dvce_ismobile, dvce_screenwidth,
            dvce_screenheight) AS "rank"
        FROM atomic.events
        WHERE domain_userid IS NOT NULL
        ) AS a
      WHERE rank = 1  
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    );

-- Finally consolidate all the individual sessions tables into a single table in the snowplow_pivots schema

DROP TABLE IF EXISTS snowplow_pivots.sessions;
CREATE TABLE snowplow_pivots.sessions 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, domain_sessionidx, session_start_ts)  -- optimized to join on other session_intermediary.session_X tables
	AS (
      SELECT
        s.domain_userid,
        s.domain_sessionidx,
        s.session_start_ts,
        s.session_end_ts,
        s.number_of_events,
        s.distinct_pages_viewed,
        g.geo_country,
        g.geo_country_code_2_characters,
        g.geo_country_code_3_characters,
        g.geo_region,
        g.geo_city,
        g.geo_zipcode,
        g.geo_latitude,
        g.geo_longitude,
        l.page_urlhost AS landing_page_host,
        l.page_urlpath AS landing_page_path,
        l2.page_urlhost AS exit_page_host,
        l2.page_urlpath AS exit_page_path,
        s2.mkt_source,
        s2.mkt_medium,
        s2.mkt_term,
        s2.mkt_campaign,
        s2.refr_source,
        s2.refr_medium,
        s2.refr_term,
        s2.refr_urlhost,
        s2.refr_urlpath,
        t.br_name,
        t.br_family,
        t.br_version,
        t.br_type,
        t.br_renderengine,
        t.br_lang,
        t.br_features_director,
        t.br_features_flash,
        t.br_features_gears,
        t.br_features_java,
        t.br_features_pdf,
        t.br_features_quicktime,
        t.br_features_realplayer,
        t.br_features_silverlight,
        t.br_features_windowsmedia,
        t.br_cookies,
        t.os_name,
        t.os_family,
        t.os_manufacturer,
        t.os_timezone,
        t.dvce_type,
        t.dvce_ismobile,
        t.dvce_screenwidth,
        t.dvce_screenheight
      FROM      snowplow_intermediary.sessions_basic        AS s
      LEFT JOIN snowplow_intermediary.sessions_geo          AS g  ON s.domain_userid = g.domain_userid  AND s.domain_sessionidx = g.domain_sessionidx 
      LEFT JOIN snowplow_intermediary.sessions_landing_page AS l  ON s.domain_userid = l.domain_userid  AND s.domain_sessionidx = l.domain_sessionidx
      LEFT JOIN snowplow_intermediary.sessions_exit_page    AS l2 ON s.domain_userid = l2.domain_userid AND s.domain_sessionidx = l2.domain_sessionidx
      LEFT JOIN snowplow_intermediary.sessions_source       AS s2 ON s.domain_userid = s2.domain_userid AND s.domain_sessionidx = s2.domain_sessionidx
      LEFT JOIN snowplow_intermediary.sessions_technology   AS t  ON s.domain_userid = t.domain_userid  AND s.domain_sessionidx = t.domain_sessionidx
	);