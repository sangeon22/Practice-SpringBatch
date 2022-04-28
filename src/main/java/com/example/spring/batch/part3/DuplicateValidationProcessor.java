package com.example.spring.batch.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DuplicateValidationProcessor<T> implements ItemProcessor<T, T> {

    private final Map<String, Object> keyPool = new ConcurrentHashMap<>();
    private final Function<T, String> keyExtractor;
    // 필터링 여부를 체크하는 필드
    private final boolean allowDuplicate;


    public DuplicateValidationProcessor(Function<T, String> keyExtractor,
                                        boolean allowDuplicate) {
        this.keyExtractor = keyExtractor;
        this.allowDuplicate = allowDuplicate;
    }

    @Override
    public T process(T item) throws Exception {
        // allowDuplicate == true 면 아이템을 그냥 return 넘겨줌
        if (allowDuplicate) {
            return item;
        }
        //키를 하나 받음
        // 해당 아이템에서 키를 추출함
        String key = keyExtractor.apply(item);

        // keyPool에 키가 존재하면 중복이니 null
        if (keyPool.containsKey(key)) {
            return null;
        }
        // 키풀에 존재하지 않는 키라면, 다음 중복체크를 위해 키를 저장(키,밸류)
        keyPool.put(key, key);
        return item;
    }
}
