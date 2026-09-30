package com.alandevise.tsdb.adapter.impl;

import com.alandevise.tsdb.config.IoTDBProperties;
import com.alandevise.tsdb.exception.*;
import com.alandevise.tsdb.model.*;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IoTDBNullValueTest {
    IoTDBTableAdapter adapter; ITableSessionPool pool; ITableSession session;
    @BeforeEach void setup() throws Exception {
        IoTDBProperties config=new IoTDBProperties(); config.setDatabase("null_contract");
        adapter=new IoTDBTableAdapter(config,config.getPool(),false);
        pool=mock(ITableSessionPool.class); session=mock(ITableSession.class); when(pool.getSession()).thenReturn(session);
        IoTDBTestPools.ready(adapter, pool, "null_contract");
    }
    static TSDBRecord row(long time,Object optional) {
        Map<String,Object> fields=new LinkedHashMap<>(); fields.put("value",10d); fields.put("optional",optional);
        return new TSDBRecord("metrics",time,Map.of("device","a"),fields);
    }
    @ParameterizedTest @ValueSource(booleans={true,false})
    void infersTypeFromNonNullRowsRegardlessOfOrderAndPreservesBitmap(boolean nullFirst) throws Exception {
        List<TSDBRecord> rows=nullFirst?List.of(row(1,null),row(2,7L)):List.of(row(1,7L),row(2,null));
        assertEquals(2,adapter.batchWriteDetailed(null,rows).committedRecords());
        ArgumentCaptor<Tablet> captor=ArgumentCaptor.forClass(Tablet.class); verify(session).insert(captor.capture()); Tablet tablet=captor.getValue();
        int index=java.util.stream.IntStream.range(0,tablet.getSchemas().size()).filter(i->tablet.getSchemas().get(i).getMeasurementName().equals("optional")).findFirst().orElseThrow();
        assertEquals(TSDataType.INT64,tablet.getSchemas().get(index).getType());
        assertTrue(tablet.isNull(nullFirst?0:1,index)); assertFalse(tablet.isNull(nullFirst?1:0,index)); assertEquals(7L,tablet.getValue(nullFirst?1:0,index));
    }
    @Test void allNullColumnIsOmittedWithoutGuessingStringSchema() throws Exception {
        assertTrue(adapter.batchWrite(null,List.of(row(1,null),row(2,null))));
        ArgumentCaptor<Tablet> captor=ArgumentCaptor.forClass(Tablet.class); verify(session).insert(captor.capture());
        assertEquals(List.of("device","value"),captor.getValue().getSchemas().stream().map(s->s.getMeasurementName()).toList());
    }
    @Test void batchWithNoKnownFieldTypeIsRejectedBeforeIo() {
        TSDBRecord invalid=new TSDBRecord("metrics",1L,Map.of("device","a"),Collections.singletonMap("optional",null));
        TSDBBatchWriteException e=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,List.of(invalid)));
        assertEquals(TSDBErrorCodeEnum.ARGUMENT_ERROR,e.getErrorCode()); assertEquals(BatchCommitStateEnum.NOT_COMMITTED,e.getResult().commitState()); verifyNoInteractions(pool,session);
    }
    @Test void nullFieldStillConflictsWithTagRole() {
        TSDBRecord invalid=new TSDBRecord("metrics",1L,Map.of("optional","tag"),Collections.singletonMap("optional",null));
        TSDBBatchWriteException e=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,List.of(invalid)));
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR,e.getErrorCode()); verifyNoInteractions(pool,session);
    }
    @Test void nullDoesNotMaskLaterNonNullTypeConflicts() {
        TSDBBatchWriteException e=assertThrows(TSDBBatchWriteException.class,()->adapter.batchWriteDetailed(null,List.of(row(1,null),row(2,7L),row(3,"string"))));
        assertEquals(TSDBErrorCodeEnum.METADATA_ERROR,e.getErrorCode()); assertEquals(0,e.getResult().committedRecords()); verifyNoInteractions(pool,session);
    }
}
