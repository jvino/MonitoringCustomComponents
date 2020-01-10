package ch.cern.alice.o2.kafka.connectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import ch.cern.alice.o2.kafka.connectors.InfluxdbUdpConsumer;



class InfluxdbUdpConsumerTest {

    private final InfluxdbUdpConsumer consumerUnderTest = new InfluxdbUdpConsumer();

    @BeforeAll
    static void beforeAllTests(){
        System.out.println("INITTTTTTTTTTTTTTTTTTTTTT");
    }

    @AfterAll
    static void afterAllTests(){
        System.out.println("BYEEEEEEEEEEEEEEEEEEEEEEE");
    }



    @Test
    @DisplayName("Provaaa!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
    void standardAssertions() {
        System.out.println("CIAOOOOOOOOOOOOOOOOOOOOOOOO");
        assertTrue( true );
        assertEquals(2, 1+1);
        assertEquals(4, 2+2, "The optional failure message is now the last parameter");
        assertTrue('a' < 'b', () -> "Assertion messages can be lazily evaluated -- "
                + "to avoid constructing complex messages unnecessarily.");
    }

    @Test
    @DisplayName("Provaaa!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
    @Disabled("Cosi'")
    void standardFAssertions() {
        System.out.println("HOLAAAAAAAAAAAAAAAAAAAAAA");
        assertTrue( true );
    }

    
}
