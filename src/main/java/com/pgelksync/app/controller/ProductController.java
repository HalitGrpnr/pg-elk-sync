package com.pgelksync.app.controller;

import com.pgelksync.app.model.ProductDto;
import com.pgelksync.app.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(path="/product")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    @PostMapping
    public ProductDto add(@RequestBody ProductDto productDto) {
        return productService.add(productDto);
    }

    @GetMapping
    public List<ProductDto> getAll() {
        return productService.getAll();
    }

    @GetMapping("/like/{name}")
    public List<ProductDto> getProductsByName(@PathVariable String name) {
        return productService.getProductsByName(name);
    }

    @GetMapping("/{name}")
    public ProductDto getProductByName(@PathVariable String name) {
        return productService.getProductByName(name);
    }
}
