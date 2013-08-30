/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import com.google.common.reflect.TypeToken;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * UnitTestPurchaseAnalytics unit test. Verifies the correctness of the app using internal unit test.
 */
public class UnitTestPurchaseAnalytics extends AppFabricTestBase {

  static Type stringMapType = new TypeToken<Map<String, String>>() {
  }.getType();
  static Type objectMapType = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Test
  public void TestDataSetUsage() throws IOException, TimeoutException, InterruptedException {
    ApplicationManager appManager = deployApplication(PurchaseAnalyticsApp.class);

    FlowManager purchaseAnalyticsFlow = appManager.startFlow("PurchaseAnalyticsFlow");
    //FlowManager generatedPurchaseAnalyticsFlow = appManager.startFlow("GeneratedPurchaseAnalyticsFlow");

    StreamWriter s1 = appManager.getStreamWriter("transactionStream");

    // Purchase
    // 1|{"customer":"alex","product":"FisherPrice","quantity":10,"price":"100","purchaseTime":"129308132"}
    s1.send("{\"customer\":\"alex\",\"product\":\"FisherPrice\",\"quantity\":10,\"price\":\"100\",\"purchaseTime\":\"129308132\", \"type\":1}");

    // Product
    // 2|{"productId":"1","description":"FisherPrice"}
    s1.send("{\"productId\":\"1\",\"description\":\"FisherPrice\": \"type\":2}");

    // Customer
    // 3|{"customerId":"1","name":"alex","zip":90210,"rating":100}
    s1.send("{\"customerId\":\"1\",\"name\":\"alex\",\"zip\":90210,\"rating\":100, \"type\":3}");

    // Test MR jobs
//    appManager.startMapReduce("PurchaseHistoryBuilder");
//    appManager.startMapReduce("RegionBuilder");
//    appManager.startMapReduce("PurchaseStatsBuilder");
  }
}
