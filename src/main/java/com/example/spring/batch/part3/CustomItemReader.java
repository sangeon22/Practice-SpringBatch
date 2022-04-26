package com.example.spring.batch.part3;

/*

Java Collection의 List를 Reader로 처리하는 ItemReader로 만듬

 */

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.List;

public class CustomItemReader<T> implements ItemReader<T> {

    private final List<T> items;

    public CustomItemReader(List<T> items) {
        this.items = new ArrayList<>(items);
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!items.isEmpty()){
            // 0번째 인덱스를 반환함과 동시에 제거
            return items.remove(0);
        }

        // items.isEmpty() - null로 리턴할 시 chunk 반복의 끝을 의미
        return null;
    }
}
