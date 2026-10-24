package com.example.shopfront.media;

import com.example.shopfront.catalog.Product;
import com.example.shopfront.catalog.ProductRepository;
import com.example.shopfront.catalog.ProductService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@RestController
@RequestMapping("/api/products/{productId}/image")
public class ProductImageController {

    private final ProductService productService;
    private final ProductRepository products;
    private final File mediaDir;

    public ProductImageController(ProductService productService, ProductRepository products,
                                  @Value("${shopfront.media-dir}") String mediaDir) {
        this.productService = productService;
        this.products = products;
        this.mediaDir = new File(mediaDir);
        this.mediaDir.mkdirs();
    }

    @PostMapping
    public Product upload(@PathVariable Long productId, @RequestParam("file") MultipartFile file) throws IOException {
        Product product = productService.get(productId);
        String fileName = UUID.randomUUID() + "-" + file.getOriginalFilename();
        file.transferTo(new File(mediaDir, fileName));
        product.setImageFile(fileName);
        return products.save(product);
    }

    @GetMapping
    public ResponseEntity<Resource> download(@PathVariable Long productId) {
        Product product = productService.get(productId);
        File image = new File(mediaDir, product.getImageFile());
        if (!image.exists()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok().contentType(MediaType.IMAGE_JPEG).body(new FileSystemResource(image));
    }
}
