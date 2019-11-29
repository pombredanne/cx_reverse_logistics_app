"""Rename columns in process."""


def get_renaming():
    """Rename Looker Data."""
    return {"0_pdt_erp_products.cod_produto": 
                "sku08",
            "0_pdt_erp_products.descricao1": 
                "product_description",
            "0_pdt_erp_products.descricao_cor": 
                "color_description",
            "0_pdt_erp_products.ean_code": 
                "ean_code",
            "variant_images.variant_link_url":
                "variant_url",
            "0_pdt_categories_tree_styles_denom.category_3_name":
                "category3",
            "0_pdt_categories_tree_styles_denom.category_4_name":
                "category4",
            "products.code_color": 
                "code_color",
            "0_pdt_styles_and_collections.description": 
                "description"}
