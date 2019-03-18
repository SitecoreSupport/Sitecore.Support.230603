namespace Sitecore.Support.ContentSearch.SolrProvider
{
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.Data.LanguageFallback;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    public class SolrDocumentBuilder : Sitecore.ContentSearch.SolrProvider.SolrDocumentBuilder
    {
        public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
        {

        }

        public override void AddItemFields()
        {
            try
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

                //fix for 230603
                //if (this.Options.IndexAllFields)
                //{
                this.Indexable.LoadAllFields();
                //}
                /* fix for 230603
                var loadedFields = new HashSet<string>(this.Indexable.Fields.Select(f => f.Id.ToString()));
                var includedFields = new HashSet<string>();
                if (this.Options.HasIncludedFields)
                {
                  includedFields = new HashSet<string>(this.Options.IncludedFields);
                }
                includedFields.ExceptWith(loadedFields);
                */
                if (IsParallel)
                {
                    var exceptions = new ConcurrentQueue<Exception>();

                    this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
                    {
                        try
                        {
                            this.CheckAndAddField(this.Indexable, f);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Enqueue(ex);
                        }
                    });

                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                    /* fix for 230603
                    if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
                    {
                      this.ParallelForeachProxy.ForEach(includedFields, this.ParallelOptions, fieldId =>
                      {
                        try
                        {
                          ID id;
                          if (ID.TryParse(fieldId, out id))
                          {
                            var field = this.Indexable.GetFieldById(id);
                            if (field != null)
                            {
                              this.CheckAndAddField(this.Indexable, field);
                            }
                          }
                        }
                        catch (Exception ex)
                        {
                          exceptions.Enqueue(ex);
                        }
                      });
                    }
                    */
                }
                else
                {
                    foreach (var field in this.Indexable.Fields)
                    {
                        this.CheckAndAddField(this.Indexable, field);
                    }
                    /* fix for 230603
                    if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
                    {
                      foreach (var fieldId in includedFields)
                      {
                        ID id;
                        if (ID.TryParse(fieldId, out id))
                        {
                          var field = this.Indexable.GetFieldById(id);
                          if (field != null)
                          {
                            this.CheckAndAddField(this.Indexable, field);
                          }
                        }
                      }
                    }
                    */
                }
            }
            finally
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
            }
        }

        private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
        {
            string name = field.Name;
            if (IsTemplate && Options.HasExcludedTemplateFields && (Options.ExcludedTemplateFields.Contains(name) || Options.ExcludedTemplateFields.Contains(field.Id.ToString())))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else if (IsMedia && Options.HasExcludedMediaFields && Options.ExcludedMediaFields.Contains(field.Name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Media field was excluded.");
            }
            else if (Options.ExcludedFields.Contains(field.Id.ToString()) || Options.ExcludedFields.Contains(name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else
            {
                try
                {
                    if (Options.IndexAllFields)
                    {
                        using (new LanguageFallbackFieldSwitcher(Index.EnableFieldLanguageFallback))
                        {
                            AddField(field);
                        }
                    }
                    else if (Options.IncludedFields.Contains(name) || Options.IncludedFields.Contains(field.Id.ToString()))
                    {
                        using (new LanguageFallbackFieldSwitcher(Index.EnableFieldLanguageFallback))
                        {
                            AddField(field);
                        }
                    }
                    else
                    {
                        VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was not included.");
                    }
                }
                catch (Exception exception)
                {
                    if (Settings.StopOnCrawlFieldError())
                    {
                        throw;
                    }
                    CrawlingLog.Log.Fatal(string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name), exception);
                }
            }
        }

    }
}
