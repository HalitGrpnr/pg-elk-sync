package com.pgelksync.app.service.impl;

import com.pgelksync.app.model.ProductDocument;
import com.pgelksync.app.model.ProductDto;
import com.pgelksync.app.repository.ProductDocumentRepository;
import com.pgelksync.app.service.ElasticsearchService;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilders;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ElasticsearchServiceImpl implements ElasticsearchService {

    private final ElasticsearchOperations elasticsearchRestTemplate;
    private final ProductDocumentRepository productDocumentRepository;
    private final ModelMapper modelMapper;

    @Override
    public List<ProductDto> getProductsByName(String query) {
        final List<ProductDocument> products = productDocumentRepository.findByName(query);
        return products.stream().map(product -> modelMapper.map(product, ProductDto.class))
                .collect(Collectors.toList());
    }

    @Override
    public List<ProductDto> getProductsByNameWithStringQuery(String query) {
        Query stringQuery = new StringQuery(
                "{\"match\":{\"name\":{\"query\":\"" + query + "\"}}}\"");

        return getProducts(stringQuery);
    }

    @Override
    public List<ProductDto> getProductsByNameWithCriteriaQuery(String query) {
        Query criteriaQuery = new CriteriaQuery(
                new Criteria("name")
                        .is(query)
                        .or("description")
                        .is(query));

        return getProducts(criteriaQuery);
    }

    @Override
    public List<ProductDto> getProductsByPriceWithCriteriaQuery(Double minPrice, Double maxPrice) {
        Query criteriaQuery = new CriteriaQuery(new Criteria("price")
                .greaterThan(minPrice)
                .lessThan(maxPrice));

        return getProducts(criteriaQuery);
    }

    @Override
    public List<ProductDto> getProductsByNameWithNativeSearchQuery(String query) {
        Query nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchQuery("name", query))
                .build();

        return getProducts(nativeSearchQuery);
    }

    @Override
    public List<ProductDto> getProductsByNameOrDescriptionWithFuzzy(String query) {
        Query searchQuery = new NativeSearchQueryBuilder()
                .withFilter(QueryBuilders
                        .multiMatchQuery(query, "name", "description")
                        .fuzziness(Fuzziness.AUTO))
                .build();

        return getProducts(searchQuery);
    }

    @Override
    public List<ProductDto> getProductSuggestions(String query) {
        Query searchQuery = new NativeSearchQueryBuilder()
                .withFilter(QueryBuilders
                        .wildcardQuery("name", query + "*"))
                .withPageable(PageRequest.of(0, 5))
                .build();

        return getProducts(searchQuery);
    }

    @Override
    public List<ProductDto> getProductSuggestionsByAllFields(String query) {
        Query searchQuery = new NativeSearchQueryBuilder()
                .withFilter(QueryBuilders
                        .boolQuery()
                        .should(QueryBuilders
                                .wildcardQuery("name", query + "*"))
                        .should(QueryBuilders
                                .wildcardQuery("description", query + "*")))
                .build();
        return getProducts(searchQuery);
    }

    @Override
    public List<ProductDto> getProductByParameters(String name, String description, Double minPrice, Double maxPrice) {
        Query searchQuery = new NativeSearchQueryBuilder()
                .withFilter(QueryBuilders
                        .boolQuery()
                        .must(QueryBuilders
                                .wildcardQuery("name", name + "*"))
                        .must(QueryBuilders
                                .wildcardQuery("description", description + "*"))
                        .must(QueryBuilders.rangeQuery("price").gte(minPrice))
                        .must(QueryBuilders.rangeQuery("price").lte(maxPrice)))
                .build();

        return getProducts(searchQuery);
    }

    private List<ProductDto> getProducts(Query query) {
        SearchHits<ProductDocument> searchHits = elasticsearchRestTemplate.search(query, ProductDocument.class);
        return searchHits.get()
                .map(result -> modelMapper.map(result.getContent(), ProductDto.class))
                .collect(Collectors.toList());
    }
}
