package com.camacuchi.kafka.valley.producers;

import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.serdes.JsonSerdes;
import com.camacuchi.kafka.valley.serdes.TransmissionsSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransmissionPublisher {
//    private final KafkaTemplate<String, Transmissions> kafkaTemplate;

//    @Bean
//    public void publishTransmissions() {

        LinkedList<Transmissions> transmissionsList = new LinkedList<>(List.of(
                new Transmissions("865532044620074","-13.000000","35.131793","kcj228x","0","Connected",29,"1700470941000"),
                new Transmissions("867157046517967","-1.472783","36.968137","KAJ697Q","0","Connected",0,"1700474342000"),
                new Transmissions("861230041892630","-1.282123","36.825537","KBC193M","0","Connected",0,"1700474338000"),
                new Transmissions("867157046554432","0.463093","35.387548","KBB142L","0","Connected",0,"1700474307000"),
                new Transmissions("867157046487112","-1.202068","36.834105","KCF510U","0","Connected",0,"1700473115000"),
                new Transmissions("867157046492138","0.720457","35.301828","KAZ033M","0","Connected",7,"1700474336000"),
                new Transmissions("867157046539284","-0.309042","36.146802","kcu041z","0","Connected",0,"1700474342000"),
                new Transmissions("861230041670226","-1.346003","36.907777","KCS877T","0","Connected",0,"1700474343000"),
                new Transmissions("865532044455596","-1.159843","36.935380","KCJ641Y","0","Connected",0,"1700474341000"),
                new Transmissions("865532044454458","-0.717927","36.432302","KCS019Z","0","Connected",32,"1700474338000"),
                new Transmissions("865532044415442","-1.299770","36.786398","KCV846H","0","Connected",25,"1700474341000"),
                new Transmissions("865532044550776","-1.448053","36.975693","KBU829T","0","Connected",0,"1700474342000"),
                new Transmissions("861230041772014","0.282607","34.754245","KCY066L","0","Connected",17,"1700474341000"),
                new Transmissions("867157046526745","-3.483922","37.687305","KCY138A","0","Connected",7,"1700474268000"),
                new Transmissions("867157046531075","-1.287858","36.833230","KBY165A","0","Connected",0,"1700474342000"),
                new Transmissions("867157046518221","-1.283330","36.843827","KCW022U","0","Connected",0,"1700474341000"),
                new Transmissions("867157046546446","-1.323727","36.870200","KCV166Z","0","Connected",0,"1700474320000"),
                new Transmissions("861230041763526","-12.000000","36.968397","KDL021H","0","Connected",0,"1700474342000"),
                new Transmissions("861230041763526","-12.000000","36.968397","KDL021H","0","Connected",0,"1700474342000"),
                new Transmissions("861230041763526","-12.000000","36.968397","KDL021H","0","Connected",0,"1700474342000"),
                new Transmissions("861230041763526","-12.000000","36.968382","KDL021H","0","Connected",0,"1700474342000"),
                new Transmissions("867157046547899","-1.520393","37.268710","KCW171W","0","Connected",0,"1700474186000"),
                new Transmissions("865532044417943","-1.216448","36.845233","KCQ099Y","0","Connected",0,"1700474188000"),
                new Transmissions("867157046532743","-0.528062","35.247910","KCU187L","0","Connected",30,"1700474340000"),
                new Transmissions("865532044445217","-11.000000","36.796772","KAK270Q","0","Connected",45,"1700474046000"),
                new Transmissions("865532044445217","-11.000000","36.796772","KAK270Q","0","Connected",46,"1700474050000"),
                new Transmissions("865532044445217","-11.000000","36.796772","KAK270Q","0","Connected",46,"1700474055000"),
                new Transmissions("865532044445217","-11.000000","36.795230","KAK270Q","0","Connected",45,"1700474061000"),
                new Transmissions("865532044453633","-1.252118","36.936392","KCG459P","0","Connected",0,"1700474338000"),
                new Transmissions("867157046531109","-0.506182","37.524177","KAY338V","0","Connected",0,"1700474342000"),
                new Transmissions("865532044414999","-1.291968","36.700522","kcy116a","0","Connected",0,"1700474341000"),
                new Transmissions("865532044427371","-1.026278","37.038362","KCY289D","0","Connected",0,"1700474340000"),
                new Transmissions("867157046562237","-1.334773","36.893753","KAG378P","0","Connected",0,"1700474340000"),
                new Transmissions("861230040899206","-3.829258","39.792022","KCN206B","0","Connected",17,"1700474332000"),
                new Transmissions("867157046536389","-0.529348","35.292173","KCU066R","0","Connected",14,"1700474341000"),
                new Transmissions("865532044493258","-0.421222","36.951312","kcf457x","0","Connected",0,"1700474043000"),
                new Transmissions("865532044427751","-0.665840","37.365447","kcy189u","0","Connected",0,"1700474341000"),
                new Transmissions("865532044465587","-1.284647","36.821748","KCR209J","0","Connected",0,"1700474341000"),
                new Transmissions("865532044535066","-1.200190","36.830468","Kcs128a","0","Connected",6,"1700474341000"),
                new Transmissions("867157046559027","-1.277563","36.877243","KCW087V","0","Connected",0,"1700474333000"),
                new Transmissions("865532044468771","-1.262718","36.878383","KAU142T","0","Connected",0,"1700474339000"),
                new Transmissions("865532044422216","-2.800322","37.537968","KCR081T","0","Connected",0,"1700474341000"),
                new Transmissions("867157046489837","-0.131832","34.743852","KCX258D","0","Connected",0,"1700474337000"),
                new Transmissions("867157046521886","-1.293507","36.816582","KCW265A","0","Connected",17,"1700473585000"),
                new Transmissions("867157046564910","-1.291882","36.841412","KBH239Y","0","Connected",0,"1700474341000"),
                new Transmissions("865532044438204","-1.277400","36.904503","KDM640D","0","Connected",0,"1700474343000"),
                new Transmissions("861230041638660","-2.755083","37.515008","KCQ776Y","0","Connected",15,"1700474340000"),
                new Transmissions("867157046529228","-3.252693","40.101980","KCW057H","0","Connected",0,"1700474339000"),
                new Transmissions("861230041692352","-1.148990","36.957377","KAT039Q","0","Connected",0,"1700474340000"),
                new Transmissions("867157046540589","-1.184012","36.935657","KCW032X","0","Connected",0,"1700474330000"),
                new Transmissions("865532044576581","-0.087543","35.672968","KCT037B","0","Connected",0,"1700473974000"),
                new Transmissions("865532044533863","-1.278368","36.651410","KBV510Q","0","Connected",0,"1700474341000"),
                new Transmissions("865532044530182","0.621987","35.468013","KCW864E","0","Connected",0,"1700474076000"),
                new Transmissions("865532044430524","-1.317447","36.701617","KDH473H","0","Connected",0,"1700474217000"),
                new Transmissions("865532044599542","-11.000000","36.858938","KCS263U","0","Connected",28,"1700470123000"),
                new Transmissions("865532044599542","-11.000000","36.858938","KCS263U","0","Connected",35,"1700470128000"),
                new Transmissions("865532044599542","-11.000000","36.858938","KCS263U","0","Connected",29,"1700470133000"),
                new Transmissions("865532044599542","-11.000000","36.853962","KCS263U","0","Connected",29,"1700470138000"),
                new Transmissions("865532044437164","-1.283920","37.096478","KCY073Z","0","Connected",0,"1700473481000"),
                new Transmissions("861230041892754","-1.192228","36.744313","KCD532K","0","Connected",0,"1700473457000"),
                new Transmissions("865532044445217","-1.331407","36.784390","KAK270Q","0","Connected",21,"1700474221000"),
                new Transmissions("865532044468540","-1.319160","36.895505","KCR057F","0","Connected",0,"1700474221000"),
                new Transmissions("867157046507570","-1.449737","36.968225","KCG226F","0","Connected",0,"1700474341000"),
                new Transmissions("865532044450894","-1.337217","36.864753","KCA420Y","0","Connected",0,"1700474333000"),
                new Transmissions("862095056239657","-1.285157","36.884132","KCB409R","0","Connected",0,"1700474267000"),
                new Transmissions("861230041634537","-1.278807","36.907340","kbz155j","0","Connected",0,"1700474334000"),
                new Transmissions("861230041696288","-1.288678","36.834542","KAK005M","0","Connected",0,"1700474309000"),
                new Transmissions("861230041696601","-1.193347","36.926373","kcv297a","0","Connected",45,"1700474342000"),
                new Transmissions("867157046491973","-1.054665","37.109440","KBV402Y","0","Connected",0,"1700474292000"),
                new Transmissions("867157046547592","-1.180987","36.827150","KCS129A","0","Connected",24,"1700474024000"),
                new Transmissions("867157046517348","-1.292652","36.890272","KCH236N","0","Connected",25,"1700473620000"),
                new Transmissions("865532044620074","-13.000000","35.131793","kcj228x","0","Connected",30,"1700470927000"),
                new Transmissions("865532044620074","-13.000000","35.131793","kcj228x","0","Connected",29,"1700470931000"),
                new Transmissions("865532044620074","-13.000000","35.131793","kcj228x","0","Connected",27,"1700470936000"),
                new Transmissions("865532044620074","-13.000000","35.131793","kcj228x","0","Connected",29,"1700470941000"),
                new Transmissions("867157046517967","-1.472783","36.968137","KAJ697Q","0","Connected",0,"1700474342000"),
                new Transmissions("861230041892630","-1.282123","36.825537","KBC193M","0","Connected",0,"1700474338000"),
                new Transmissions("867157046554432","0.463093","35.387548","KBB142L","0","Connected",0,"1700474307000"),
                new Transmissions("867157046487112","-1.202068","36.834105","KCF510U","0","Connected",0,"1700473115000"),
                new Transmissions("865532044431167","-0.599732","34.900765","KCT629Y","0","Connected",46,"1700474197000"),
                new Transmissions("865532044442859","-1.297807","36.803625","KCZ173E","0","Connected",0,"1700474306000"),
                new Transmissions("867157046521696","-1.227513","36.797893","KCR081H","0","Connected",17,"1700474313000"),
                new Transmissions("867157046516084","-1.277903","36.830537","KCX389D","0","Connected",0,"1700474219000"),
                new Transmissions("861230041634107","0.028135","36.365523","KCM927L","0","Connected",9,"1700474311000"),
                new Transmissions("867157046555371","-1.281415","36.822543","KBL978Y","0","Connected",0,"1700473799000"),
                new Transmissions("865532044435416","-1.281917","36.822830","KCY167C","0","Connected",0,"1700474174000"),
                new Transmissions("865532044444541","-1.291710","36.808578","KCM670Q","0","Connected",0,"1700474312000"),
                new Transmissions("861230041659732","-1.286215","36.834065","KBS357V","0","Connected",0,"1700474311000"),
                new Transmissions("867157046537791","-3.309380","40.092258","KCV257Z","0","Connected",0,"1700474094000"),
                new Transmissions("865532044438063","-1.293335","36.847278","KCX264S","0","Connected",0,"1700474304000"),
                new Transmissions("865532044430672","-1.287042","36.894530","kcy034c","0","Connected",0,"1700473838000"),
                new Transmissions("867157046507828","0.111228","35.471342","kcv958h","0","Connected",11,"1700474307000"),
                new Transmissions("867157046551545","-0.396188","34.946535","KCW087N","0","Connected",0,"1700474310000"),
                new Transmissions("865532044534036","-1.288145","36.753245","kbm720d","0","Connected",0,"1700474193000"),
                new Transmissions("867157046549598","-1.261505","36.733958","KCT201Y","0","Connected",0,"1700474312000"),
                new Transmissions("867157046545620","-3.223630","40.119238","KCT095W","0","Connected",10,"1700474313000"),
                new Transmissions("861230041665267","-1.281582","36.818418","KAG850G","0","Connected",0,"1700474311000"),
                new Transmissions("867157046553517","-1.305587","36.876620","KCC825W","0","Connected",0,"1700474313000"),
                new Transmissions("867157046540779","-1.680470","37.194990","KCK121F","0","Connected",58,"1700474313000"),
                new Transmissions("861230041634263","-1.285668","36.762908","KDG128Y","0","Connected",0,"1700474307000"),
                new Transmissions("861230041660508","-1.255658","36.690493","KCP072D","0","Connected",0,"1700474191000"),
                new Transmissions("867157046511028","-1.151480","36.933387","KCS194A","0","Connected",2,"1700474253000"),
                new Transmissions("867157046538591","-1.282130","36.829608","KAU907H","0","Connected",0,"1700474313000"),
                new Transmissions("867157046551016","-1.241020","36.876070","KCR181T","0","Connected",0,"1700474239000"),
                new Transmissions("865532044415665","-1.285667","36.833987","KCY092C","0","Connected",0,"1700473754000"),
                new Transmissions("862057045043384","-1.285893","36.880895","KCZ079C","1","Connected",0,"1700474313000"),
                new Transmissions("865532044432082","-1.149473","36.947005","KCY054U","0","Connected",26,"1700472402000"),
                new Transmissions("865532044426431","-1.365777","38.007233","KAX112W","0","Connected",12,"1700474315000"),
                new Transmissions("867157046556981","-1.237393","36.673860","KCH016Z","0","Connected",20,"1700474312000"),
                new Transmissions("861230041744740","-1.287792","36.817870","KBT879H","0","Connected",16,"1700474248000"),
                new Transmissions("861230041639908","-1.166407","36.953283","KBR574P","0","Connected",110,"1700474308000"),
                new Transmissions("867157046540589","-1.180385","36.939107","KCW032X","0","Connected",0,"1700474299000"),
                new Transmissions("867157046487609","-1.289282","36.805782","kby004m","0","Connected",0,"1700473942000"),
                new Transmissions("861230041741829","0.462190","35.388375","KBM207D","0","Connected",0,"1700474315000"),
                new Transmissions("867157046546446","-1.323727","36.870200","KCV166Z","0","Connected",0,"1700474294000"),
                new Transmissions("867157046521464","-1.293017","36.820652","KBR221S","0","Connected",0,"1700474262000"),
                new Transmissions("867157046516985","-1.297695","36.763873","KCJ037Q","0","Connected",0,"1700474310000"),
                new Transmissions("867157046526224","-1.284172","36.826327","KBH815Q","0","Connected",0,"1700474311000"),
                new Transmissions("867157046537635","-1.291532","36.823227","KAQ834L","0","Connected",0,"1700474111000"),
                new Transmissions("861230041637753","-1.315048","36.720178","KBX363G","0","Connected",0,"1700474312000"),
                new Transmissions("865532044447650","-1.533768","37.132730","kch326b","0","Connected",14,"1700474313000"),
                new Transmissions("865532044411300","-0.770015","36.599257","KCS053L","0","Connected",0,"1700474313000"),
                new Transmissions("865532044438196","-1.241310","36.658015","KCY017K","0","Connected",11,"1700474306000"),
                new Transmissions("861230041692337","-0.717243","37.158660","KCL349C","0","Connected",48,"1700473816000"),
                new Transmissions("865532044535066","-1.200133","36.830525","Kcs128a","0","Connected",0,"1700474315000"),
                new Transmissions("861230041636086","-1.307480","36.803593","KCB399L","0","Connected",20,"1700474308000"),
                new Transmissions("865532044429203","-1.287215","36.828828","KCK376G","0","Connected",20,"1700474313000"),
                new Transmissions("865532044534705","-1.247065","36.694275","KCW232L","0","Connected",80,"1700474315000"),
                new Transmissions("865532044453526","-1.350755","36.905683","kbm790h","0","Connected",58,"1700474309000"),
                new Transmissions("865532044436414","-1.298837","36.761153","KCY013C","0","Connected",100,"1700473857000"),
                new Transmissions("861230040899255","-1.494507","37.060570","KCN205B","0","Connected",89,"1700474311000"),
                new Transmissions("865532044433593","-1.266857","36.846005","KCA802Z","0","Connected",0,"1700474314000")

                ));
//        transmissionsList.forEach(transmissions ->
//                kafkaTemplate.send(EValleyTopics.TOPIC_TRANSMISSIONS.getName(), transmissions));


//    }

    @Bean
    public Supplier<Message<Transmissions>> publishTransmissions() {
        return () -> {
            if (transmissionsList.peek() != null) {
                Message<Transmissions> o = MessageBuilder
                        .withPayload(transmissionsList.peek())
                        .setHeader(KafkaHeaders.KEY, Objects.requireNonNull(transmissionsList.poll()).imei())
                        .build();
                log.info("Transmissions: {}", o.getPayload());
                return o;
            } else {
                return null;
            }
        };
    }

    @Bean
    public Consumer<KStream<String, Transmissions>> consumer() {
        return transactions -> transactions
                .map((key, transmission) -> KeyValue.pair(transmission.imei(), transmission))
                .filter((key, transmission) ->  Objects.requireNonNull(transmission.speed()) > 80)
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .reduce((current, aggregate) -> aggregate,
                        Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>as("over_speeding")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.Transmissions()))
                .toStream().peek((k, v) -> log.info("Overspeeding: {}", v)).to("over_speeding");
    }
}
