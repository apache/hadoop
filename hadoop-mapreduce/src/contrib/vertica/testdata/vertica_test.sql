--/**
-- * Licensed to the Apache Software Foundation (ASF) under one
-- * or more contributor license agreements.  See the NOTICE file
-- * distributed with this work for additional information
-- * regarding copyright ownership.  The ASF licenses this file
-- * to you under the Apache License, Version 2.0 (the
-- * "License"); you may not use this file except in compliance
-- * with the License.  You may obtain a copy of the License at
-- *
-- *     http://www.apache.org/licenses/LICENSE-2.0
-- *
-- * Unless required by applicable law or agreed to in writing, software
-- * distributed under the License is distributed on an "AS IS" BASIS,
-- * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- * See the License for the specific language governing permissions and
-- * limitations under the License.
-- */

drop table mrsource cascade;
create table mrsource (
  key int,
  value varchar(10)
);

drop table bar cascade;
create table bar(
  y int
);

select implement_temp_design('');

insert into mrsource values(0, 'zero');
insert into mrsource values(1, 'one');
insert into mrsource values(2, 'two');
insert into mrsource values(3, 'three');
insert into mrsource values(4, 'four');
insert into mrsource values(5, 'five');

insert into bar values(0);
insert into bar values(1);
insert into bar values(2);
insert into bar values(3);
insert into bar values(4);
insert into bar values(5);

commit;
select advance_epoch();
