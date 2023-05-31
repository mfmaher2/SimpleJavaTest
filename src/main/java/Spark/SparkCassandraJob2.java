package Spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.col;

public class SparkCassandraJob2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Cassandra Job")
                .master("local") // Use this for local mode
                //.master("spark://masterURL:7077") // Use this for a standalone cluster
                .getOrCreate();


        Dataset df_acct_cnt = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "cam_account_contact_l1_ks")
                .option("table", "account_contact")
                .load();

        Dataset df_cust_acct = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "cam_account_l1_ks")
                .option("table", "cust_acct_v1")
                .load();

        // Perform the join operation with explicit column specification
        Dataset joinedDf = df_acct_cnt.alias("a").join(df_cust_acct.alias("b"), df_acct_cnt.col("account_number").equalTo(df_cust_acct.col("account_number")), "inner");

        Dataset finalDf = joinedDf.select(col("a.account_number"), col("a.opco"),
                col("contact_type_code"), col("contact_business_id"),
                col("address__additional_line1"), col("address__country_code"),
                col("address__geo_political_subdivision1"), col("address__geo_political_subdivision2"),
                col("address__geo_political_subdivision3"), col("address__postal_code"),
                col("address__street_line"), col("company_name"),
                col("contact_document_id"), col("email"),
                col("person__first_name"), col("person__last_name"), col("person__middle_name"),
                col("share_id"), col("credit_detail__cash_only_reason"),
                col("credit_detail__credit_rating"), col("invoice_preference__billing_restriction_indicator"),
                col("profile__account_type"), col("profile__airport_code"),
                col("profile__archive_reason_code"), col("profile__customer_account_status"),
                col("profile__interline_cd"), col("profile__synonym_name_1"), col("profile__synonym_name_2"));

        finalDf.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "cam_search_l1_ks")
                .option("table", "cam_search_v1")
                .mode(SaveMode.Append)
                .save();

        Dataset df_cam_search = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace","cam_search_l1_ks")
                .option("table", "cam_search_v1")
                .load();

        System.out.println("Count: " + df_cam_search.count());
    }
}
