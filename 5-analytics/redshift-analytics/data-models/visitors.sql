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

-- Visitors basic table contains a line per individual website visitor
-- The standard model identifies visitors using only a first party cookie

-- First create a basic table with simple information per visitor that can be derived from a single table scan

DROP TABLE IF EXISTS snowplow_intermediary.visitors_basic;
CREATE TABLE snowplow_intermediary.visitors_basic 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
  AS (
  SELECT
    domain_userid,
    MIN(collector_tstamp) AS first_touch,
    MAX(collector_tstamp) AS last_touch,
    COUNT(*) AS number_of_events,
    COUNT(DISTINCT page_urlpath) AS distinct_pages_viewed,
    MAX(domain_sessionidx) AS number_of_sessions
  FROM atomic.events 
  GROUP BY 1
);

-- Second combine that table with the different sessions intermediary tables to put together the complete visitors table

DROP TABLE IF EXISTS snowplow_pivots.visitors;
CREATE TABLE snowplow_pivots.visitors 
	DISTKEY (domain_userid)  -- optimized to join on other session_intermediary.session_X tables
	SORTKEY (domain_userid, last_touch)  -- optimized to join on other session_intermediary.session_X tables
  AS (
  SELECT
    v.domain_userid,
    v.first_touch,
    v.last_touch,
    v.number_of_events,
    v.distinct_pages_viewed,
    v.number_of_sessions,
    l.page_urlhost,
    l.page_urlpath,
    s.mkt_source,
    s.mkt_medium,
    s.mkt_campaign,
    s.mkt_term,
    s.refr_source,
    s.refr_medium,
    s.refr_term,
    s.refr_urlhost,
    s.refr_urlpath
  FROM
    snowplow_intermediary.visitors_basic                  AS v
    LEFT JOIN snowplow_intermediary.sessions_landing_page AS l ON v.domain_userid = l.domain_userid AND l.domain_sessionidx = 1
    LEFT JOIN snowplow_intermediary.sessions_source       AS s ON v.domain_userid = s.domain_userid AND s.domain_sessionidx = 1
);