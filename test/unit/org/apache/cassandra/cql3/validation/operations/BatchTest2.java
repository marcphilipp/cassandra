/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.validation.operations;

import org.apache.cassandra.cql3.CQLTester;
import org.junit.Test;

public class BatchTest2 extends CQLTester
{

    @Test
    public void testBatchStaticTTLConditionalInteraction() throws Throwable
    {

        createTable(String.format("CREATE TABLE %s.clustering_static (\n" +
                "  id int,\n" +
                "  clustering1 int,\n" +
                "  clustering2 int,\n" +
                "  clustering3 int,\n" +
                "  sval int static, \n" +
                "  val int, \n" +
                " PRIMARY KEY(id, clustering1, clustering2, clustering3)" +
                ")", KEYSPACE));

        execute("DELETE FROM " + KEYSPACE +".clustering_static WHERE id=1");

        String clusteringInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s); ";
        String clusteringTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) USING TTL %s; ";
        String clusteringStaticInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, sval, val) VALUES(%s, %s, %s, %s, %s, %s); ";
        String clusteringConditionalInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s) IF NOT EXISTS; ";
        String clusteringConditionalTTLInsert = "INSERT INTO " + KEYSPACE + ".clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(%s, %s, %s, %s, %s)  IF NOT EXISTS USING TTL %s; ";
        String clusteringUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringStaticUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET sval=%s WHERE id=%s ;";
        String clusteringTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringStaticConditionalUpdate = "UPDATE " + KEYSPACE + ".clustering_static SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;";
        String clusteringConditionalTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ;";
        String clusteringStaticConditionalTTLUpdate = "UPDATE " + KEYSPACE + ".clustering_static USING TTL %s SET val=%s WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ;";
        String clusteringStaticConditionalStaticUpdate = "UPDATE " + KEYSPACE +".clustering_static SET sval=%s WHERE id=%s IF sval=%s; ";
        String clusteringDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s ;";
        String clusteringRangeDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s ;";
        String clusteringConditionalDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF val=%s ; ";
        String clusteringStaticConditionalDelete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s ; ";


        execute("BEGIN BATCH " + String.format(clusteringStaticInsert, 1, 1, 1, 1, 1, 1) + " APPLY BATCH");

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"), row(1, 1, 1, 1, 1, 1));

        StringBuilder cmd2 = new StringBuilder();
        cmd2.append("BEGIN BATCH ");
        cmd2.append(String.format(clusteringInsert, 1, 1, 1, 2, 2));
        cmd2.append(String.format(clusteringStaticConditionalUpdate, 11, 1, 1, 1, 1, 1));
        cmd2.append("APPLY BATCH ");
        execute(cmd2.toString());


        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 1, 1, 11),
                row(1, 1, 1, 2, 1, 2)
        );


        StringBuilder cmd3 = new StringBuilder();
        cmd3.append("BEGIN BATCH ");
        cmd3.append(String.format(clusteringInsert, 1, 1, 2, 3, 23));
        cmd3.append(String.format(clusteringStaticUpdate, 22, 1));
        cmd3.append(String.format(clusteringDelete, 1, 1, 1, 1));
        cmd3.append("APPLY BATCH ");
        execute(cmd3.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 2),
                row(1, 1, 2, 3, 22, 23)
        );

        StringBuilder cmd4 = new StringBuilder();
        cmd4.append("BEGIN BATCH ");
        cmd4.append(String.format(clusteringInsert, 1, 2, 3, 4, 1234));
        cmd4.append(String.format(clusteringStaticConditionalTTLUpdate, 5, 234, 1, 1, 1, 2, 22));
        cmd4.append("APPLY BATCH ");
        execute(cmd4.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 234),
                row(1, 1, 2, 3, 22, 23),
                row(1, 2, 3, 4, 22, 1234)
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, null),
                row(1, 1, 2, 3, 22, 23),
                row(1, 2, 3, 4, 22, 1234)
        );

        StringBuilder cmd5 = new StringBuilder();
        cmd5.append("BEGIN BATCH ");
        cmd5.append(String.format(clusteringRangeDelete, 1, 2));
        cmd5.append(String.format(clusteringStaticConditionalUpdate, 1234, 1, 1, 1, 2, 22));
        cmd5.append("APPLY BATCH ");
        execute(cmd5.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1234),
                row(1, 1, 2, 3, 22, 23)
        );

        StringBuilder cmd6 = new StringBuilder();
        cmd6.append("BEGIN BATCH ");
        cmd6.append(String.format(clusteringUpdate, 345, 1, 3, 4, 5));
        cmd6.append(String.format(clusteringStaticConditionalUpdate, 1, 1, 1, 1, 2, 22));
        cmd6.append("APPLY BATCH ");
        execute(cmd6.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1),
                row(1, 1, 2, 3, 22, 23),
                row(1, 3, 4, 5, 22, 345)
        );


        StringBuilder cmd7 = new StringBuilder();
        cmd7.append("BEGIN BATCH ");
        cmd7.append(String.format(clusteringDelete, 1, 3, 4, 5));
        cmd7.append(String.format(clusteringStaticConditionalUpdate, 2300, 1, 1, 2, 3, 1));  // SHOULD NOT MATCH
        cmd7.append("APPLY BATCH ");
        execute(cmd7.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 1, 1, 2, 22, 1),
                row(1, 1, 2, 3, 22, 23),
                row(1, 3, 4, 5, 22, 345)
        );

        StringBuilder cmd8 = new StringBuilder();
        cmd8.append("BEGIN BATCH ");
        cmd8.append(String.format(clusteringConditionalDelete, 1, 3, 4, 5, 345));
        cmd8.append(String.format(clusteringRangeDelete, 1, 1));
        cmd8.append(String.format(clusteringInsert, 1, 2, 3, 4, 5));
        cmd8.append("APPLY BATCH ");
        execute(cmd8.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5)
        );

        StringBuilder cmd9 = new StringBuilder();
        cmd9.append("BEGIN BATCH ");
        cmd9.append(String.format(clusteringConditionalInsert, 1, 3, 4, 5, 345));
        cmd9.append(String.format(clusteringDelete, 1, 2, 3, 4));
        cmd9.append("APPLY BATCH ");
        execute(cmd9.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 345)
        );

        StringBuilder cmd10 = new StringBuilder();
        cmd10.append("BEGIN BATCH ");
        cmd10.append(String.format(clusteringTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd10.append(String.format(clusteringConditionalTTLUpdate, 10, 5, 1, 3, 4, 5, 345));
        cmd10.append("APPLY BATCH ");
        execute(cmd10.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5), // 5 second TTL
                row(1, 3, 4, 5, 22, 5)  // 10 second TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 5) // now 4 second TTL
        );

        StringBuilder cmd11 = new StringBuilder();
        cmd11.append("BEGIN BATCH ");
        cmd11.append(String.format(clusteringConditionalTTLInsert, 1, 2, 3, 4, 5, 5));
        cmd11.append(String.format(clusteringInsert,1, 4, 5, 6, 7));
        cmd11.append("APPLY BATCH ");
        execute(cmd11.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5), // This one has 5 seconds left
                row(1, 3, 4, 5, 22, 5), // This one should have 4 seconds left
                row(1, 4, 5, 6, 22, 7) // This one has no TTL
        );

        Thread.sleep(6000);

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, null), // We had a row here before from cmd9, but we've ttl'd out the value in cmd11
                row(1, 4, 5, 6, 22, 7)
        );

        StringBuilder cmd12 = new StringBuilder();
        cmd12.append("BEGIN BATCH ");
        cmd12.append(String.format(clusteringConditionalTTLUpdate, 5, 5, 1, 3, 4, 5, null));
        cmd12.append(String.format(clusteringTTLUpdate, 5, 8, 1, 4, 5, 6));
        cmd12.append("APPLY BATCH ");
        execute(cmd12.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, 5),
                row(1, 4, 5, 6, 22, 8)
        );

        Thread.sleep(6000);
        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 3, 4, 5, 22, null),
                row(1, 4, 5, 6, 22, null)
        );

        StringBuilder cmd13 = new StringBuilder();
        cmd13.append("BEGIN BATCH ");
        cmd13.append(String.format(clusteringStaticConditionalDelete, 1, 3, 4, 5, 22));
        cmd13.append(String.format(clusteringInsert, 1, 2, 3, 4, 5));
        cmd13.append("APPLY BATCH ");
        execute(cmd13.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 22, 5),
                row(1, 4, 5, 6, 22, null)
        );

        StringBuilder cmd14 = new StringBuilder();
        cmd14.append("BEGIN BATCH ");
        cmd14.append(String.format(clusteringStaticConditionalStaticUpdate, 23, 1, 22));
        cmd14.append(String.format(clusteringDelete, 1, 4, 5, 6));
        cmd14.append("APPLY BATCH ");
        execute(cmd14.toString());

        assertRows(execute("SELECT * FROM " + KEYSPACE+".clustering_static WHERE id=1"),
                row(1, 2, 3, 4, 23, 5)
        );
    }

}
